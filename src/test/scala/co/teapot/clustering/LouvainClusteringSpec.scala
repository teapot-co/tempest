package co.teapot.clustering

import java.util.Random

import co.teapot.graph.DynamicDirectedGraph
import co.teapot.util.CollectionUtil
import org.scalatest.{FlatSpec, Matchers}

class LouvainClusteringSpec extends FlatSpec with Matchers {
  val cliqueRingGraph = DynamicDirectedGraph()
  for (i <- 0 until 30) {
    // Create clique i on nodes {i, i + 1, ..., i + 4}
    for (j <- 0 until 5) {
      for (k <- j + 1 until 5) {
        cliqueRingGraph.addEdge(5 * i + j, 5 * i + k)
        cliqueRingGraph.addEdge(5 * i + k, 5 * i + j)
      }
    }
    cliqueRingGraph.addEdge(5 * i + 4, (5 * (i + 1)) % 150)
    cliqueRingGraph.addEdge((5 * (i + 1)) % 150, 5 * i + 4)
  }
  //for (u <- cliqueRingGraph.nodeIds.toArray.sorted) {
  //  for (v <- cliqueRingGraph.outNeighbors(u).sorted) {
  //    println(s"$u $v")
  //  }
  //}

  "LouvainClusterer" should "cluster cliques in a ring " in {
    val clusterer = new SerialLouvainClusterer(cliqueRingGraph)
    val recursiveClustering = clusterer.cluster()
    val clusterings = recursiveClustering.clusterings

    clusterings.size should be (2)
    val expectedClusterSizes = Seq(5, 2)
    for ((clustering, expectedSize) <- clusterings zip expectedClusterSizes) {
      val clusterToSize = CollectionUtil.efficientIntIntMap().withDefaultValue(0)
      for ((u, c) <- clustering) {
        //println(s"$i: $c")
        clusterToSize(c) += 1
      }
      for (c <- clusterToSize.keys)
        clusterToSize(c) should equal (expectedSize)
    }

    val nodes = recursiveClustering.nodes
    val nodeToCluster1 = new Array[Int](nodes.size)
    val nodeToCluster2 = new Array[Int](nodes.size)
    for (u <- nodes) {
      nodeToCluster1(u) = recursiveClustering.clustersOfNode(u)(0)
      nodeToCluster2(u) = recursiveClustering.clustersOfNode(u)(1)

    }

    clusterer.computeModularity(nodeToCluster1) shouldEqual (0.876 +- 0.001)
    clusterer.computeModularity(nodeToCluster2) shouldEqual (0.888 +- 0.001)
  }

  "LouvainClusterer" should "cluster planted approximate cliques" in {
    // Create a graph with 100 nodes and 4 approximate cliques
    val n = 100
    val cliqueCount = 4
    def nodeToCluster(i: Int): Int = i / (n / cliqueCount)
    val insideClusterEdgeProb = 0.5f
    val betweenClusterEdgeProb = 0.1f

    val random = new Random(42) // Seed for consistent testing
    val graph = DynamicDirectedGraph()
    for (i <- 0 until n) {
      for (j <- 0 until n) {
        val edgeProb = if (nodeToCluster(i) == nodeToCluster(j))
          insideClusterEdgeProb
        else
          betweenClusterEdgeProb
        if (random.nextFloat() < edgeProb)
          graph.addEdge(i, j)
      }
    }
    val clusterer = new SerialLouvainClusterer(graph)
    val clustering = clusterer.cluster()
    val nodeToEmpericalCluster = new Array[Int](n)
    for (i <- 0 until n) {
      nodeToEmpericalCluster(i) = clustering.clustersOfNode(i).last
    }
    for (i <- 0 until n) {
      for (j <- 0 until n) {
        val expectedInSameCluster = (nodeToCluster(i) == nodeToCluster(j))
        val actualInSameCluster = (nodeToEmpericalCluster(i) == nodeToEmpericalCluster(j))
        expectedInSameCluster shouldEqual (actualInSameCluster)
      }
    }
  }
}
