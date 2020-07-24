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

  /** Request to perform garbage collection */
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

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case o: Operation => root ! o
    case GC =>
      val empty = createRoot
      root ! CopyTo(empty)
      context.become(garbageCollecting(empty))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case o: Operation => pendingQueue = pendingQueue.enqueue(o)
    case CopyFinished =>
      pendingQueue.foreach(newRoot ! _)
      root ! PoisonPill
      root = newRoot
      pendingQueue = Queue.empty
      context.become(normal)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Contains(req, id, e) =>
      if e != elem || (e == elem && removed) then subtrees.get(if e <= elem then Left else Right) match
        case Some(s) => s ! Contains(req, id, e)
        case None => req ! ContainsResult(id, result = false)
      else req ! ContainsResult(id, result = true)

    case Insert(req, id, e) =>
      if e != elem || (e == elem && removed) then subtrees.get(if e <= elem then Left else Right) match
        case Some(s) => s ! Insert(req, id, e)
        case None =>
          subtrees += ((if e <= elem then Left else Right) -> context.actorOf(BinaryTreeNode.props(e, initiallyRemoved = false)))
          req ! OperationFinished(id)
      else req ! OperationFinished(id)

    case Remove(req, id, e) =>
      if e != elem || (e == elem && removed) then subtrees.get(if e <= elem then Left else Right) match
        case Some(s) => s ! Remove(req, id, e)
        case None => req ! OperationFinished(id)
      else
        removed = true
        req ! OperationFinished(id)

    case CopyTo(newRoot) =>
      if !removed then newRoot ! Insert(self, elem, elem)
      subtrees.values.foreach(_ ! CopyTo(newRoot))
      if subtrees.isEmpty && removed then context.parent ! CopyFinished else context.become(copying(subtrees.values.toSet, insertConfirmed = removed))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive =
    case OperationFinished(_) => 
      if !expected.isEmpty then context.become(copying(expected, insertConfirmed = true)) else context.parent ! CopyFinished
    case CopyFinished =>
      val a = expected - sender
      if a.isEmpty && insertConfirmed then context.parent ! CopyFinished else context.become(copying(a, insertConfirmed))

}
