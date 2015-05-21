/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => processOperation(op)
    case GC =>
      val newRoot = createRoot
//      println("copy start")
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC =>
      // do nothing
    case op: Operation =>
//      println("get operation during gc: " + op)
      pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished =>
//      println("tree copy finished")
//      println(s"kill root actor $sender")
      assert(root == sender)
      root ! PoisonPill
      root = newRoot
      while (pendingQueue.nonEmpty) {
        val t = pendingQueue.dequeue
//        println("get pending operation: " + t)
        processOperation(t._1)
        pendingQueue = t._2
      }
      context.become(normal)
  }

  private def processOperation(op: Operation): Unit = {
//    println("process operation: " + op)
    op match {
      case Insert(requester, id, elem) =>
        root ! Insert(requester, id, elem)
      case Contains(requester, id, elem) =>
        root ! Contains(requester, id, elem)
      case Remove(requester, id, elem) =>
        root ! Remove(requester, id, elem)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)


}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, elem1) =>
      if (elem1 == elem) {
        if (removed) {
          removed = false
        }
        sendBack(requester, OperationFinished(id))
      } else if (elem1 > elem) {
        if (subtrees.contains(Right)) {
          subtrees(Right) ! Insert(requester, id, elem1)
        } else {
          subtrees += Right -> context.actorOf(props(elem1, initiallyRemoved = false))
          sendBack(requester, OperationFinished(id))
        }
      } else {
        if (subtrees.contains(Left)) {
          subtrees(Left) ! Insert(requester, id, elem1)
        } else {
          subtrees += Left -> context.actorOf(props(elem1, initiallyRemoved = false))
          sendBack(requester, OperationFinished(id))
        }
      }
    case Contains(requester, id, elem1) =>
      if (elem1 == elem) {
        if (removed) {
          sendBack(requester, ContainsResult(id, result = false))
        } else {
          sendBack(requester, ContainsResult(id, result = true))
        }
      } else if (elem1 > elem) {
        if (subtrees.contains(Right)) {
          subtrees(Right) ! Contains(requester, id, elem1)
        } else {
          sendBack(requester, ContainsResult(id, result = false))
        }
      } else {
        if (subtrees.contains(Left)) {
          subtrees(Left) ! Contains(requester, id, elem1)
        } else {
          sendBack(requester, ContainsResult(id, result = false))
        }
      }
    case Remove(requester, id, elem1) =>
      if (elem1 == elem) {
        removed = true
        sendBack(requester, OperationFinished(id))
      } else if (elem1 > elem) {
        if (subtrees.contains(Right)) {
          subtrees(Right) ! Remove(requester, id, elem1)
        } else {
          sendBack(requester, OperationFinished(id))
        }
      } else {
        if (subtrees.contains(Left)) {
          subtrees(Left) ! Remove(requester, id, elem1)
        } else {
          sendBack(requester, OperationFinished(id))
        }
      }
    case CopyTo(treeNode) =>
      if (!removed) {
        treeNode ! Insert(self, 0, elem)
      }
      var expected = Set.empty[ActorRef]
      if (subtrees.contains(Left)) {
        subtrees(Left) ! CopyTo(treeNode)
        expected += subtrees(Left)
      }
      if (subtrees.contains(Right)) {
        subtrees(Right) ! CopyTo(treeNode)
        expected += subtrees(Right)
      }
      val insertConfirmed = if (removed) true else false
      if (insertConfirmed && expected.isEmpty) {
        copyFinished()
      } else {
        context.become(copying(expected, insertConfirmed = insertConfirmed))
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case _: OperationFinished =>
//      println(s"get operation finished $sender, $expected, $insertConfirmed")
      if (expected.isEmpty) {
        copyFinished()
      } else {
        context.become(copying(expected, insertConfirmed = true))
      }
    case CopyFinished =>
//      println(s"get children copy finished $sender, $expected, $insertConfirmed")
//      println(s"kill children actor $sender")
      sender ! PoisonPill
      if (expected == Set(sender()) && insertConfirmed) {
        copyFinished()
      } else {
        context.become(copying(expected - sender(), insertConfirmed = insertConfirmed))
      }
  }

  private def sendBack(requester: ActorRef, reply: OperationReply): Unit = {
//    println(s"$self send to $requester, $reply")
    requester ! reply
  }

  private def copyFinished(): Unit = {
//    println("copy finished of current node: " + self)
    context.parent ! CopyFinished
//    context.stop(self)
  }

}
