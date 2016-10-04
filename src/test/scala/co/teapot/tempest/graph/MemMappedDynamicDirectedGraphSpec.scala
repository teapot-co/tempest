package co.teapot.tempest.graph

import java.io.File
import co.teapot.tempest.util.LogUtil
import org.scalatest.{FlatSpec, Matchers}

class MemMappedDynamicDirectedGraphSpec extends FlatSpec with Matchers {
  LogUtil.configureConsoleLog("")

  "MemMappedDynamicDirectedGraphSpec" should "behave correctly" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    println(s"File size: " + f.length())
    val g = new MemMappedDynamicDirectedGraph(f)
    val testEdges = Array((1, 4), (5, 3), (1, 5), (1, 3))

    g.nodeCount should equal (0)

    for (((id1, id2), i) <- testEdges.zipWithIndex) {
      g.edgeCount should be (i)
      g.addEdge(id1, id2)
    }

    val gSimple = DynamicDirectedGraph(testEdges)
    for (u <- gSimple.nodeIds) {
      g.outNeighbors(u) should contain theSameElementsAs (gSimple.outNeighbors(u))
      g.outDegree(u) should equal (gSimple.outDegree(u))
      g.inNeighbors(u) should contain theSameElementsAs (gSimple.inNeighbors(u))
      g.inDegree(u) should equal (gSimple.inDegree(u))
    }

    g.maxNodeId should equal (5)
    g.nodeCount should equal (6)

    a[NoSuchElementException] should be thrownBy {
      g.outNeighbors(6)
    }
    a[NoSuchElementException] should be thrownBy {
      g.inDegree(6)
    }
  }

  /* This test uses 48GB of RAM
  it  should "support node ids between 2**31 and 2**32" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    println(s"File size: " + f.length())
    val g = new MemMappedDynamicDirectedGraph(f)
    g.mmAllocator.syncAllWrites = false // To make test faster
    g.addEdge(42, -3)
    g.outNeighbors(42) should contain theSameElementsAs (Seq(-3))
    g.inNeighbors(-3) should contain theSameElementsAs (Seq(42))
    println(s"nodeCount ${g.nodeCount} vs ${Integer.toUnsignedLong(-3) + 1}")
  }
  */
}
