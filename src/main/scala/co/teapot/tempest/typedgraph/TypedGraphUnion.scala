package co.teapot.tempest.typedgraph

/** Given a list of TypedGraphs, creates a view of their union. */
/* Note: Much of this class is very similar to code in DirectedGraphUnion - is there a way to avoid code duplication? */
class TypedGraphUnion(typedGraphs: Seq[TypedGraph]) extends TypedGraph {
  // Return a Traversable to avoid materializing a Seq of Nodes (potentially saving memory)
  override def neighbors(node: Node): Traversable[Node] = new Traversable[Node] {
    def foreach[A](f: Node => A): Unit = {
      for (typedGraph <- typedGraphs) {
        typedGraph.neighbors(node).foreach(f)
      }
    }
  }

  override def neighbor(node: Node, i: Int): Node = {
    var adjustedI = i // i - (cumulative degree of node in previous graphs)
    for (graph <- typedGraphs) {
      val d = graph.degree(node)
      if (adjustedI < d) {
        return graph.neighbor(node, adjustedI)
      } else {
        adjustedI -= d
      }
    }
    throw new IndexOutOfBoundsException(s"index $i invalid for neighbor of $node having degree ${degree(node)}")
  }

  /**
    * Returns the degree (# neighbors) of the given node. Returns 0 if the given node is not part of this graph (wrong
    * type, or too large of tempestId)
    */
  override def degree(node: Node): Int = (typedGraphs map (g => g.degree(node))).sum
}
