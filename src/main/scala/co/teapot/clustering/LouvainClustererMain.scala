package co.teapot.clustering

import java.io.{FileWriter, BufferedWriter}

import co.teapot.graph.MemoryMappedDirectedGraph

object LouvainClustererMain {
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("Usage: java -cp <tempest.jar> co.teapot.clustering.LouvainClusterMain <binary graph> <output cluster file>")
      System.exit(1)
    }
    val graphName = args(0)
    val outputFile = args(1)
    val debug = args.size > 2 // Temporary: Extra parameter indicates debug
    val graph = MemoryMappedDirectedGraph(graphName)
    val clusterings = new SerialLouvainClusterer(graph, debug).cluster()
    printClusterings(outputFile, clusterings)
  }

  def printClusterings(outputFile: String, clustering: HierarchicalClustering): Unit = {
    // First write node to cluster file
    val out = new BufferedWriter(new FileWriter(outputFile))
    out.write("{\n")
    for ((u, i) <- clustering.nodes.zipWithIndex) {
      out.write(s"$u: " + clustering.clustersOfNode(u).mkString("[", ", ", "]"))
      if (i + 1 < clustering.nodes.size)
        out.write(",\n")
    }
    out.write("\n}")
    out.close()
  }
}
