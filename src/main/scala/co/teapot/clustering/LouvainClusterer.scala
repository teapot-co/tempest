package co.teapot.clustering

import java.util.Date

import co.teapot.graph.DirectedGraph
import co.teapot.graph.experimental.{DirectedGraphAsUnweightedGraph, WeightedUndirectedGraph, MutableWeightedUndirectedGraph, HashMapMutableWeightedUndirectedGraph}
import co.teapot.util.CollectionUtil
import it.unimi.dsi.fastutil.ints.IntArrayList
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A class for Louvain Clustering, as described in the paper "Fast unfolding of communities in large networks."
  * The given graph is interpreted as an undirected graph, using only its out-neighbors.
  */
class SerialLouvainClusterer(originalGraph: DirectedGraph) {
  val n = originalGraph.maxNodeId + 1
  val m = originalGraph.edgeCount / 2.0 // Divide by two because we're interpreting a directed graph as undirected

  /** Clusters the given graph, and returns a sequence of maps mapping ids to cluster ids.
    * The first map maps node ids in the original graph to 1st level cluster ids.
    * The second map maps 1st level cluster ids to 2nd level cluster ids.
    * ...The ith map maps (i-1)th level cluster ids to ith level cluster ids.
    */
  def cluster(): Seq[collection.Map[Int, Int]] = {
    val firstGraph = new DirectedGraphAsUnweightedGraph(originalGraph)
    clusterRecursively(firstGraph)
  }

  def clusterRecursively(currentGraph: WeightedUndirectedGraph): Seq[collection.Map[Int, Int]] = {
    doPass(currentGraph) match {
      case Some(clustering) =>
        val nextGraph = clusteringToMergedNodeGraph(currentGraph, clustering)
        Seq(clustering) ++ clusterRecursively(nextGraph)
      case None =>
        Seq.empty
    }
  }

  /**
    * Clusters the given previousGraph and returns a map from node to cluster id, or None if no single
    * merge would include the modularity of the graph.
    */
  def doPass(previousGraph: WeightedUndirectedGraph): Option[collection.Map[Int, Int]] = {
    println(s"Starting pass at ${new Date()}")
    // The results depends on the order nodes are visited; for consistency, sort nodes.
    val sortedNodes = previousGraph.nodes.toArray.sorted

    // Stores the current cluster assignment of a given node
    val nodeToCluster = CollectionUtil.efficientIntIntMap()
    // Stores the total weight of edges from nodes in a cluster
    val clusterToTotalEdgeWeight = CollectionUtil.efficientIntDoubleMap()

    for (u <- previousGraph.nodes) {
      // Initially assign each node to its own cluster
      nodeToCluster(u) = u
      clusterToTotalEdgeWeight(u) = previousGraph.totalWeight(u)
    }

    var someNodeEverMoved = false
    var someNodeMoved = true
    while (someNodeMoved) {
      someNodeMoved = false

      for (u <- sortedNodes) {
        val edgeWeightU = previousGraph.totalWeight(u)

        // Consider moving node u to a new cluster
        // First compute the edge weight from u to each cluster neighboring u
        val clusterToEdgeWeightU = CollectionUtil.efficientIntDoubleMap().withDefaultValue(0.0f)
        clusterToEdgeWeightU(nodeToCluster(u)) = 0.0f // Staying where u already is is always an option
        for ((v, weightUV) <- previousGraph.neighborsWithWeights(u)
             if v != u) {
          val vCluster = nodeToCluster(v)
          clusterToEdgeWeightU(vCluster) = clusterToEdgeWeightU(vCluster) + weightUV
        }

        // The score of a cluster is the change in objective value if we move node u to it
        // See equation (2) in Fast Unfolding of Communities in Large Networks
        def score(cluster: Int): Double = {
          val edgeWeightWithoutU = clusterToTotalEdgeWeight(cluster) -
            (if (cluster == nodeToCluster(u)) edgeWeightU else 0)
          val s = clusterToEdgeWeightU(cluster) / m -
            2.0 * edgeWeightWithoutU * edgeWeightU / square(2.0 * m)
          if (LouvainClusterer.Debug)
            println(s"from $u to $cluster, weight-from-$u, internal-weight, score: " +
              (clusterToEdgeWeightU(cluster), edgeWeightWithoutU, s))
          s
        }
        val bestNewCluster = clusterToEdgeWeightU.keys maxBy score


        //println(s" best new cluster, current: $bestNewCluster ${nodeToCluster(u)}")
        if (score(bestNewCluster) > score(nodeToCluster(u)) + LouvainClusterer.MinImprovement) {
          if (LouvainClusterer.Debug) println (s"  Moving node $u to cluster $bestNewCluster")
          someNodeMoved = true
          someNodeEverMoved = true
          //currentModularity += (score(bestNewCluster) - score(nodeToCluster(i)))
          //println(s"Modularity increased to $currentModularity")
          clusterToTotalEdgeWeight(nodeToCluster(u)) -= edgeWeightU
          nodeToCluster(u) = bestNewCluster
          clusterToTotalEdgeWeight(bestNewCluster) += edgeWeightU
        }
      }
    }
    if (someNodeEverMoved)
      Some(nodeToCluster)
    else
      None
  }

  def clusteringToMergedNodeGraph(previousGraph: WeightedUndirectedGraph,
                                  nodeToCluster: Int => Int
                                  ): MutableWeightedUndirectedGraph = {
    val result = new HashMapMutableWeightedUndirectedGraph()
    for (u <- previousGraph.nodes) {
      for ((v, weight) <- previousGraph.neighborsWithWeights(u)) {
        result.increaseEdgeWeight(nodeToCluster(u), nodeToCluster(v), weight)
      }
    }
    result
  }

  def square(x: Double): Double = x * x

  def computeModularity(nodeToCluster: Array[Int]): Double = {
    val clusterToNodes = new mutable.HashMap[Int, IntArrayList]()
    for ((c, i) <- nodeToCluster.zipWithIndex) {
      clusterToNodes.getOrElseUpdate(c, new IntArrayList()).add(i)
    }

    var modularity = 0.0
    for (u <- originalGraph.nodeIds) {
      for (v <- originalGraph.outNeighbors(u)) {
        if (nodeToCluster(u) == nodeToCluster(v)) {
          modularity += 1.0
        }
      }
    }
    for ((c, nodes) <- clusterToNodes) {
      for (u <- nodes.asScala) {
        for (v <- nodes.asScala) {
          modularity -= originalGraph.outDegree(u) * originalGraph.outDegree(v) / (2.0 * m)
        }
      }
    }

    modularity / (2.0 * m)
  }
}

object LouvainClusterer {
  val MinImprovement = 1.0e-6
  val Debug = false
}

