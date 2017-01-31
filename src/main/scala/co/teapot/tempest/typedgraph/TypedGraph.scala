package co.teapot.tempest.typedgraph

import scala.util.Random

/** A graph where each node has a type. */
/* This class is still being developed.  For now it is undirected. */
trait TypedGraph {

  /**
   * Returns the degree (# neighbors) of the given node. Returns 0 if the given node is not part of this graph (wrong
   * type, or too large of tempestId)
   */
  def degree(node: Node): Int

  // Return a Traversable to avoid materializing a Seq of Nodes (potentially saving memory)
  def neighbors(node: Node): Traversable[Node]

  def neighbor(node: Node, i: Int): Node

  def randomNeighbor(node: Node, random: Random = Random.self): Node = {
    neighbor(node, random.nextInt(degree(node)))
  }
}
