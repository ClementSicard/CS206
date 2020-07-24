package lockfree

import scala.annotation.tailrec

class SortedList extends AbstractSortedList {

  // The sentinel node at the head.
  private val _head: Node = createNode(0, None, isHead = true)

  // The first logical node is referenced by the head.
  def firstNode: Option[Node] = _head.next

  // Finds the first node whose value satisfies the predicate.
  // Returns the predecessor of the node and the node.
  def findNodeWithPrev(pred: Int => Boolean): (Node, Option[Node]) =
    
    @tailrec 
    def helper(p: Node, c: Option[Node]): (Node, Option[Node]) = (p, c) match {
      case (prev, Some(curr)) =>
        if (curr.deleted) then
          prev.atomicState.compareAndSet((c, false), (curr.next, false))
          findNodeWithPrev(pred)
        else if pred(curr.value) then (p, c) else 
          helper(curr, curr.next)
      case (last, None) => (last, None)
    }
    
    helper(_head, firstNode)

  // Insert an element in the list.
  def insert(e: Int): Unit =
    val (p, n) = findNodeWithPrev(_ >= e)
    val curr = createNode(e, n)
    if !p.atomicState.compareAndSet((n, false), (Some(curr), false)) then insert(e)

  // Checks if the list contains an element.
  def contains(e: Int): Boolean = findNodeWithPrev(_ == e)._2 match {
    case Some(n) => !n.deleted
    case _ => false
  }

  // Delete an element from the list.
  // Should only delete one element when multiple occurences are present.
  def delete(e: Int): Boolean = findNodeWithPrev(_ == e)._2 match {
    case Some(n) => if n.mark then true else delete(e)
    case _ => false
  }
}
