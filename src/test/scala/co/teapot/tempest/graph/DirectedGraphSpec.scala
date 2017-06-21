package co.teapot.tempest.graph

import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable

class DirectedGraphSpec extends FlatSpec with Matchers {
  "DirectedGraph" should "behave correctly on simple graphs" in {
    val graph = DirectedGraph(1 -> 2, 1 -> 3)
    val outList = graph.outNeighborList(1)
    outList.size should equal (2)
    outList.get(0) should equal (new Integer(2))
    outList.get(1) should equal (new Integer(3))

    graph.neighbors(1, EdgeDirOut) should contain theSameElementsAs (Seq(2, 3))
    graph.neighbors(1, EdgeDirIn) should contain theSameElementsAs (Seq.empty)
    graph.neighbors(2, EdgeDirOut) should contain theSameElementsAs (Seq.empty)
    graph.neighbors(2, EdgeDirIn) should contain theSameElementsAs (Seq(1))

    graph.distinctInNeighbors(1) should contain theSameElementsAs (Seq.empty)
    graph.distinctOutNeighbors(1) should contain theSameElementsAs (Seq(2,3))

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

  "DirectedGraph" should "behave correctly on multigraphs" in {
    val graph = DirectedGraph(1 -> 2, 1 -> 3, 1 -> 2, 4 -> 2)
    val outList = graph.outNeighborList(1)
    outList.size should equal (3)
    outList.get(0) should equal (new Integer(2))
    outList.get(1) should equal (new Integer(3))

    graph.neighbors(1, EdgeDirOut) should contain theSameElementsAs (Seq(2, 3, 2))
    graph.neighbors(1, EdgeDirIn) should contain theSameElementsAs (Seq.empty)
    graph.neighbors(2, EdgeDirOut) should contain theSameElementsAs (Seq.empty)
    graph.neighbors(2, EdgeDirIn) should contain theSameElementsAs (Seq(1, 1, 4))

    graph.distinctInNeighbors(1) should contain theSameElementsAs (Seq.empty)
    graph.distinctOutNeighbors(1) should contain theSameElementsAs (Seq(2,3))

    graph.distinctInNeighbors(2) should contain theSameElementsAs (Seq(1,4))
    graph.distinctOutNeighbors(1) should contain theSameElementsAs (Seq(2,3))

    val inList = graph.inNeighborList(2)
    inList.size should equal (3)
    inList.get(0) should equal (new Integer(1))

    graph.outNeighborCount(1) should equal (2)
    graph.inNeighborCount(2) should equal (2)

    new mutable.HashSet() ++ (0 to 40 map { i => graph.uniformRandomOutNeighbor(1) }) should
      contain theSameElementsAs (Seq(2, 3))

    var twoCount = 0
    var threeCount = 0
    for (i <- 1 to 5000) {
      val randomNeighbor = graph.uniformRandomDistinctOutNeighbor(1)
      if (randomNeighbor == 2) {
        twoCount += 1
      } else
        threeCount += 1
    }

    // Check that graph.uniformRandomDistinctOutNeighbors randomly chooses between distinct
    // neighbors, and NOT over non-distinct neighbors. In other words, twoCount should
    // roughly equal threeCount in this case.
    twoCount.toDouble should equal (threeCount.toDouble +- (0.1*threeCount))

    var oneCount = 0
    var fourCount = 0
    for (i <- 1 to 5000) {
      val randomNeighbor = graph.uniformRandomDistinctInNeighbor(2)
      if (randomNeighbor == 1) {
        oneCount += 1
      } else
        fourCount += 1
    }

    // Check that graph.uniformRandomDistinctInNeighbors randomly chooses between distinct
    // neighbors, and NOT over non-distinct neighbors. In other words, oneCount should
    // roughly equal fourCount in this case.
    oneCount.toDouble should equal (fourCount.toDouble +- (0.1*fourCount))


    a[NoSuchElementException] should be thrownBy {
      graph.uniformRandomInNeighbor(1)
    }
  }
}
