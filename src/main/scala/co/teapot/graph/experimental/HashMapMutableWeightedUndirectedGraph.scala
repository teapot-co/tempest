package co.teapot.graph.experimental

import co.teapot.util.CollectionUtil

import scala.collection.mutable

private[teapot] class HashMapMutableWeightedUndirectedGraph extends MutableWeightedUndirectedGraph {
  val nodeToNeighborToWeightMap = new mutable.HashMap[Int, mutable.Map[Int, Float]]()
  val nodeToTotalWeight = CollectionUtil.efficientIntFloatMap().withDefaultValue(0.0f)

  def neighborsWithWeights(node: Int): Iterable[(Int, Float)] =
    nodeToNeighborToWeightMap(node)

  def totalWeight(node: Int): Float =
    nodeToTotalWeight(node)

  def nodes: Iterable[Int] = nodeToNeighborToWeightMap.keys

  def increaseEdgeWeight(u: Int, v: Int, weightChange: Float): Unit = {
    addNode(u)
    addNode(v)
    nodeToNeighborToWeightMap(u)(v) += weightChange
    nodeToTotalWeight(u) += weightChange
    if (v != u) {  // For self loops, we don't want to double-add
      nodeToNeighborToWeightMap(v)(u) += weightChange
      nodeToTotalWeight(v) += weightChange
    }
  }

  def addNode(u: Int): Unit = {
    if (! nodeToNeighborToWeightMap.contains(u))
      nodeToNeighborToWeightMap(u) = CollectionUtil.efficientIntFloatMap().withDefaultValue(0.0f)
  }
}
