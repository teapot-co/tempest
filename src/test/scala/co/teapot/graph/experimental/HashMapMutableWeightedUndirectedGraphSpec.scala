package co.teapot.graph.experimental

import org.scalatest.{Matchers, FlatSpec}


class HashMapMutableWeightedUndirectedGraphSpec extends FlatSpec with Matchers {
  "A weighted undirected graph" should "behave correctly" in {
    val graph = new HashMapMutableWeightedUndirectedGraph()
    graph.increaseEdgeWeight(1, 3, 1.3f)
    graph.increaseEdgeWeight(3, 5, 3.5f)
    graph.increaseEdgeWeight(3, 3, 3.3f) // Test self loop
    graph.neighborsWithWeights(1) should contain theSameElementsAs Seq((3, 1.3f))
    graph.neighborsWithWeights(3) should contain theSameElementsAs Seq((1, 1.3f), (5, 3.5f), (3, 3.3f))
    graph.totalWeight(3) should equal (1.3f + 3.3f + 3.5f)
    graph.totalWeight(5) should equal (3.5f)
  }
}
