package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply


  case class OneMinuteTimeOut(id: Long, sender: ActorRef)

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // current expected sequence number
  var expectedSeq = 0L
  // persistence actor
  val persistence = context.actorOf(persistenceProps)
  var acks = Map.empty[Long, (ActorRef, Persist)]
  // id -> (sender, replicators)
  var waitingRefs = Map.empty[Long, (ActorRef, Set[ActorRef])]
  var replicateAcks = Map.empty[ActorRef, (Long, Map[String, String])]
  context.system.scheduler.schedule(100.milliseconds, 100.milliseconds, self, Timeout)

  var currentId = 0L

  override def preStart(): Unit = {
    arbiter ! Join
  }

  override val supervisorStrategy = OneForOneStrategy() {
    case _: PersistenceException => Restart // reconnect to DB
  }



  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Replicas(replicas) =>

      // replicate new joined secondary replicas
      replicas.filter(_ != self).filterNot(secondaries.contains).foreach { replica =>
        val replicator = context.actorOf(Replicator.props(replica))

        if (kv.nonEmpty) {
          kv.foreach { case (key, value) =>
            replicator ! Replicate(key, Some(value), currentId)
          }
          replicateAcks += replicator -> (currentId, kv)
        }
        secondaries += replica -> replicator
        replicators += replicator
      }

      // stop replication of the secondary replicas that have left
      val removedReplicas = secondaries.keys.filterNot(replicas.contains)
      removedReplicas.foreach { replica =>
        val replicator = secondaries(replica)
        replicator ! PoisonPill
        secondaries -= replica
        replicators -= replicator

        var toRemove = Set.empty[Long]
        waitingRefs = waitingRefs.map {
          case (id, (sender, reps)) if reps.contains(replicator) =>
            // if current replicator is the only ack to wait
            if (reps.size == 1) {
              sender ! OperationAck(id)
              toRemove += id
            }
            id -> (sender, reps)
          case t => t
        }

        toRemove.foreach(waitingRefs -= _)
      }

    case Insert(key, value, id) =>
      currentId = id
      kv += key -> value
      persist(key, Some(value), id, sender())
    case Remove(key, id) =>
      currentId = id
      kv -= key
      persist(key, None, id, sender())
    case Get(key, id) =>
      currentId = id
      sender() ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      if (acks.contains(id)) {
        handleAck(id, persistence)
        acks -= id
      }
    case Replicated(key, id) =>
      if (replicateAcks.contains(sender())) {
        require(replicateAcks(sender())._1 == id, "must handle replica message first because id is small")
        replicateAcks = replicateAcks.updated(sender(), (id, replicateAcks(sender())._2 - key))
        if (replicateAcks(sender())._2.isEmpty) {
          replicateAcks -= sender()
        }
      } else {
        handleAck(id, sender())
      }
    case Timeout =>
      collectAck()
    case OneMinuteTimeOut(id, sender) =>
      if (waitingRefs.contains(id)) {
        waitingRefs -= id
        acks -= id
        sender ! OperationFailed(id)
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)
    case Snapshot(key, valueOption, seq) =>
      if (seq > expectedSeq) {
        // just ignore the message
      } else if (seq < expectedSeq) {
        // send ack
        sender() ! SnapshotAck(key, seq)
      } else {
        if (valueOption.isDefined) {
          kv = kv.updated(key, valueOption.get)
        } else {
          kv -= key
        }
        persistence ! Persist(key, valueOption, seq)
        acks += seq -> (sender(), Persist(key, valueOption, seq))
      }
    case Persisted(key, seq) =>
      if (acks.contains(seq)) {
        val ack = acks(seq)
        ack._1 ! SnapshotAck(key, seq)
        acks -= seq
        expectedSeq += 1
      } else {
        println(s"unexpected seq $seq")
      }

    case Timeout =>
      collectAck()
  }


  private def collectAck(): Unit = {
    acks.foreach { case (id, (_, msg)) =>
      persistence ! msg
    }
  }

  private def persist(key: String, valueOption: Option[String], id: Long, sender: ActorRef): Unit = {
    var refs = Set.empty[ActorRef]
    // send persist message
    persistence ! Persist(key, valueOption, id)
    refs += persistence
    acks += id -> (sender, Persist(key, valueOption, id))

    // send replicate messages
    replicators.foreach { replicator =>
//      println(s"send replicate to replicator $key, $id")
      replicator ! Replicate(key, valueOption, id)
      refs += replicator
    }

    waitingRefs += id -> (sender, refs)
    context.system.scheduler.scheduleOnce(1.second, self, OneMinuteTimeOut(id, sender))
  }

  private def handleAck(id: Long, sender: ActorRef): Unit = {
    // if it is already time out, do nothing
    if (!waitingRefs.contains(id)) {
      return
    }

    var refs = waitingRefs(id)._2
    refs -= sender
    waitingRefs = waitingRefs.updated(id, (waitingRefs(id)._1, refs))
    // if this message is the last to wait
    if (waitingRefs(id)._2.isEmpty) {
      waitingRefs(id)._1 ! OperationAck(id)
      waitingRefs -= id
    }
  }

}

