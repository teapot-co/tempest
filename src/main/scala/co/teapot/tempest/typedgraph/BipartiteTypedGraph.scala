package co.teapot.tempest.typedgraph

import co.teapot.tempest.graph.DirectedGraph

/** A SimpleTypedGraph has a single source node type and single target node type.  It is allowed for the
  * source and target type to be equal, but in this case it will double-count neighbors which are both
  * in-neighbors and out-neighbors, both in the neighbor call and in the degree call. */
case class BipartiteTypedGraph(sourceNodeType: String, targetNodeType: String, graph: DirectedGraph) extends TypedGraph {
  def outDegree(node: IntNode): Int = {
    if (node.`type` == sourceNodeType) {
      graph.outDegreeOr0(node.tempestId)
    } else {
      0
    }
  }

  def inDegree(node: IntNode): Int = {
    if (node.`type` == targetNodeType) {
      graph.inDegreeOr0(node.tempestId)
    } else {
      0
    }
  }

  def degree(node: IntNode): Int =
    // Note: Because we might have (sourceNodeType == targetNodeType), we need a sum here (not just disjoint case analysis)
    outDegree(node) + inDegree(node)


  def outNeighbors(node: IntNode): Traversable[IntNode] = new Traversable[IntNode] {
    def foreach[A](f: IntNode => A): Unit = {
      // In use cases like the union graph, we might give this simple graph a node whose type doesn't match, in which
      // case outNeighbors should do nothing.
      if (node.`type` == sourceNodeType) {
        for (v <- graph.outNeighbors(node.tempestId)) {
          f(new IntNode(targetNodeType, v))
        }
      }
    }
  }

  def inNeighbors(node: IntNode): Traversable[IntNode] = new Traversable[IntNode] {
    def foreach[A](f: IntNode => A): Unit = {
      if (node.`type` == targetNodeType) {
        for (v <- graph.inNeighbors(node.tempestId)) {
          f(new IntNode(sourceNodeType, v))
        }
      }
    }
  }

  def neighbors(node: IntNode): Traversable[IntNode] = new Traversable[IntNode] {
    def foreach[A](f: IntNode => A): Unit = {
      outNeighbors(node).foreach(f)
      inNeighbors(node).foreach(f)
    }
  }


  def neighbor(node: IntNode, i: Int): IntNode = {
    require(i < degree(node))
    if (i < outDegree(node)) {
      IntNode(targetNodeType, graph.outNeighbor(node.tempestId, i))
    } else {
      val adjustedI = i - outDegree(node)
      IntNode(sourceNodeType, graph.inNeighbor(node.tempestId, adjustedI))
    }
  }
}
