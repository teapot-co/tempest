package experiments

import co.teapot.tempest.graph.MemMappedDynamicDirectedGraph

object PrintNeighbors {
  def main(args: Array[String]): Unit = {
    val graph = MemMappedDynamicDirectedGraph(args(0))
    val id = Integer.parseUnsignedInt(args(1))
    println(s"outNeighbors($id): ${graph.outNeighbors(id).mkString(", ")}")
    println(s"inNeighbors($id): ${graph.inNeighbors(id).mkString(", ")}")
  }
}
