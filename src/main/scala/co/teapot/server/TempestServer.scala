package co.teapot.server

import java.util

import co.teapot.graph.{DirectedGraph, MemoryMappedDirectedGraph, TempestService}
import co.teapot.thriftbase.TeapotThriftLauncher
import co.teapot.util.LogUtil
import org.apache.thrift.TProcessor

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestServer(graph: DirectedGraph) extends TempestService.Iface {
  override def outDegree(id: Int): Int = graph.outDegree(id)

  override def inDegree(id: Int): Int = graph.inDegree(id)

  override def outNeighbors(id: Int): util.List[Integer] = graph.outNeighborList(id)

  override def inNeighbors(id: Int): util.List[Integer] = graph.inNeighborList(id)

  override def outNeighbor(id: Int, i: Int): Int = graph.outNeighbor(id, i)

  override def inNeighbor(id: Int, i: Int): Int = graph.inNeighbor(id, i)

  override def edgeCount(): Long = graph.edgeCount

  override def nodeCount(): Int = graph.nodeCount

  override def maxNodeId(): Int = graph.maxNodeId
}

object TempestServer {
  def getProcessor(graphFileName: String): TProcessor = {
    val graph = MemoryMappedDirectedGraph(graphFileName)
    val server = new TempestServer(graph)
    new TempestService.Processor(server)
  }

  def main(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args, getProcessor, "")
  }
}
