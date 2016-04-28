package co.teapot.clustering

/**
  * Represents a hierarchical clustering using a sequence of maps mapping ids to cluster ids.
  * The first map maps node ids in the original graph to 1st level cluster ids.
  * The second map maps 1st level cluster ids to 2nd level cluster ids.
  * ...The ith map maps (i-1)th level cluster ids to ith level cluster ids.
  *
  * The id of a cluster is the id of an arbitrary node in that cluster, so the same cluster id
  * might be used for multiple clusters at multiple levels of the hierarchy.
  */
class HierarchicalClustering(val clusterings: Seq[collection.Map[Int, Int]]) {
  val nodes: Array[Int] = clusterings(0).keys.toArray.sorted
  def clustersOfNode(u: Int): Seq[Int] = {
    var clusterId = u
    for (clustering <- clusterings) yield {
      clusterId = clustering(clusterId)
      clusterId
    }
  }
}

