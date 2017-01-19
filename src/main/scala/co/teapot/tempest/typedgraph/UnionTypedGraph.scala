package co.teapot.tempest.typedgraph

import co.teapot.tempest.Node

/** Given a list of TypedGraphs, creates a view of their union. */
class UnionTypedGraph(typedGraphs: Seq[TypedGraph]) extends TypedGraph {
  // Return a Traversable to avoid materializing a Seq of Nodes (potentially saving memory)
  override def outNeighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      for (typedGraph <- typedGraphs) {
        typedGraph.outNeighbors(node).foreach(f)
      }
    }
  }

  override def inNeighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      for (typedGraph <- typedGraphs) {
        typedGraph.inNeighbors(node).foreach(f)
      }
    }
  }
}

/*
Alternate design, slightly less flexible, but more efficient for unions of many graphs. We should delete this code once
we determine the above code has sufficient performance.

class UnionTypedGraph(typedGraphs: Seq[SimpleTypedGraph]) extends TypedGraph {
  val outGraphsForType = new mutable.HashMap[String, mutable.Buffer[SimpleTypedGraph]]().withDefaultValue(mutable.Buffer.empty)
  val inGraphsForType = new mutable.HashMap[String, mutable.Buffer[SimpleTypedGraph]]().withDefaultValue(mutable.Buffer.empty)
  for (g <- typedGraphs) {
    outGraphsForType.getOrElseUpdate(g.sourceNodeType, new ArrayBuffer[SimpleTypedGraph]()) += g
    inGraphsForType.getOrElseUpdate(g.targetNodeType, new ArrayBuffer[SimpleTypedGraph]()) += g
  }

  // Return a Traversable to avoid materializing a Seq of Nodes (potentially saving memory)
  override def outNeighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      for (typedGraph <- outGraphsForType(node.`type`)) {
        typedGraph.outNeighbors(node).foreach(f)
      }
    }
  }

  override def inNeighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      for (typedGraph <- inGraphsForType(node.`type`)) {
        typedGraph.inNeighbors(node).foreach(f)
      }
    }
  }
}
*/
