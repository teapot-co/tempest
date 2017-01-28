package co.teapot.tempest.typedgraph

import scala.util.Random

sealed case class IntNode(`type`: String, tempestId: Int)

/** A graph where each node has a type. */
/* This class is still being developed.  For now it is undirected. */
trait TypedGraph {

  /**
   * Returns the degree (# neighbors) of the given node. Returns 0 if the given node is not part of this graph (wrong
   * type, or too large of tempestId)
   */
  def degree(node: IntNode): Int

  // Return a Traversable to avoid materializing a Seq of Nodes (potentially saving memory)
  def neighbors(node: IntNode): Traversable[IntNode]

  def neighbor(node: IntNode, i: Int): IntNode

  def randomNeighbor(node: IntNode, random: Random = Random.self): IntNode = {
    neighbor(node, random.nextInt(degree(node)))
  }
}
