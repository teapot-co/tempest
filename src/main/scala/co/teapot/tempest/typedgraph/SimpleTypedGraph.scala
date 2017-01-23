package co.teapot.tempest.typedgraph

import co.teapot.tempest.Node
import co.teapot.tempest.graph.DirectedGraph

/** A SimpleTypedGraph has a single source node type and single target node type. */
case class SimpleTypedGraph(sourceNodeType: String, targetNodeType: String, graph: DirectedGraph) extends TypedGraph {
  override def outNeighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      // In use cases like the union graph, we might give this simple graph a node whose type doesn't match, in which
      // case outNeighbors should do nothing.
      if (node.`type` == sourceNodeType) {
        for (v <- graph.outNeighbors(node.id)) {
          f(new Node(targetNodeType, v))
        }
      }
    }
  }

  override def inNeighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      if (node.`type` == targetNodeType) {
        for (v <- graph.inNeighbors(node.id)) {
          f(new Node(sourceNodeType, v))
        }
      }
    }
  }
}
