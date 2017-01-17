package co.teapot.tempest.typedgraph

import co.teapot.tempest.Node

/** A graph where each node has a type. */
/* This class is still being developed. */
trait TypedGraph {
 // Return a Traversable to avoid materializing a Seq of Nodes (potentially saving memory)
  def outNeighbors(node: Node): Traversable[Node]

  def inNeighbors(node: Node): Traversable[Node]

  /** Traverses over both out and in neighbors of the given node. Will traverse a node twice if it is both an out-neighbor
    * and an in-neighbor.
    */
  def neighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      outNeighbors(node).foreach(f)
      inNeighbors(node).foreach(f)
    }
  }
}
