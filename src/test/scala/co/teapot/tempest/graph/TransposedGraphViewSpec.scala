package co.teapot.tempest.graph

import org.scalatest.{FlatSpec, Matchers}

class TransposedGraphViewSpec extends FlatSpec with Matchers {
  "A TransposedGraphView" should "behave correctly" in {
    val graph = DirectedGraph(1 -> 2, 1 -> 3)
    val transpose = graph.transposeView
    transpose.inNeighbors(1) should contain theSameElementsInOrderAs Seq(2, 3)
    transpose.outNeighbors(1) should contain theSameElementsInOrderAs Seq()
    transpose.outNeighbors(2) should contain theSameElementsInOrderAs Seq(1)

    transpose.inDegree(1) should equal (2)
    transpose.outDegree(1) should equal (0)
    transpose.outDegree(2) should equal (1)

    transpose.inNeighbor(1, 0) should equal (2)
    transpose.outNeighbor(2, 0) should equal (1)

    // Make sure DirectedGraph methods we didn't override still work.
    transpose.inNeighborList(1).get(0) should equal (2)
    transpose.uniformRandomOutNeighbor(2) should equal (1)
  }
}
