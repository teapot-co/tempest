package co.teapot.graph.experimental

import co.teapot.graph.DirectedGraph

/**
  * Implemented temporarily for use by the clustering algorithm.  May become a public graph API after more thought.
  */
private[teapot] trait WeightedUndirectedGraph {
  def neighborsWithWeights(node: Int): Iterable[(Int, Float)]

  def nodes: Iterable[Int]

  def totalWeight(node: Int): Float
}

private[teapot] trait MutableWeightedUndirectedGraph extends WeightedUndirectedGraph {
  def increaseEdgeWeight(u: Int, v: Int, weightChange: Float): Unit
}

private[teapot] class DirectedGraphAsUnweightedGraph(graph: DirectedGraph) extends WeightedUndirectedGraph {
  def neighborsWithWeights(node: Int): Iterable[(Int, Float)] =
    graph.outNeighbors(node) zip Array.fill(graph.outDegree(node))(1.0f)

  def nodes: Iterable[Int] = graph.nodeIds

  def totalWeight(node: Int): Float = graph.outDegree(node).toFloat
}
