package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.util.Timeout
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  // TODO: do batch here
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  context.system.scheduler.schedule(100.milliseconds, 100.milliseconds, self, Timeout)

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) =>
      acks += _seqCounter -> (sender(), Replicate(key, valueOption, id))
      replica ! Snapshot(key, valueOption, nextSeq)
    case SnapshotAck(key, seq) =>
      if (acks.contains(seq)) {
        val replicate = acks(seq)._2
        acks(seq)._1 ! Replicated(key, replicate.id)
        acks -= seq
      }
    case Timeout =>
      acks.foreach { case (seq, (_, replicate)) =>
        replica ! Snapshot(replicate.key, replicate.valueOption, seq)
      }
  }

}
