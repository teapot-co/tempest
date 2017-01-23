package co.teapot.tempest.graph

import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable

class DirectedGraphSpec extends FlatSpec with Matchers {
  "DirectedGraph" should "behave correctly on the methods it implements" in {
    val graph = DirectedGraph(1 -> 2, 1 -> 3)
    val outList = graph.outNeighborList(1)
    outList.size should equal (2)
    outList.get(0) should equal (new Integer(2))
    outList.get(1) should equal (new Integer(3))

    graph.neighbors(1, EdgeDirOut) should contain theSameElementsAs (Seq(2, 3))
    graph.neighbors(1, EdgeDirIn) should contain theSameElementsAs (Seq.empty)
    graph.neighbors(2, EdgeDirOut) should contain theSameElementsAs (Seq.empty)
    graph.neighbors(2, EdgeDirIn) should contain theSameElementsAs (Seq(1))

    val inList = graph.inNeighborList(2)
    inList.size should equal (1)
    inList.get(0) should equal (new Integer(1))

    new mutable.HashSet() ++ (0 to 40 map { i => graph.uniformRandomOutNeighbor(1) }) should
      contain theSameElementsAs (Seq(2, 3))

    graph.uniformRandomInNeighbor(2) should equal (1)

    a[NoSuchElementException] should be thrownBy {
      graph.uniformRandomInNeighbor(1)
    }
  }
}
