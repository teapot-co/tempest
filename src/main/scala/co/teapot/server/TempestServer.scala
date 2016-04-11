package co.teapot.server

import java.util

import co.teapot.graph.{BidirectionalPPRParams, DirectedGraph, MemoryMappedDirectedGraph, TempestService}
import co.teapot.thriftbase.TeapotThriftLauncher
import co.teapot.util.LogUtil
import co.teapot.util.tempest.CollectionUtil
import org.apache.thrift.TProcessor
import soal.ppr.BidirectionalPPREstimator
import soal.util.UniformDistribution

import scala.util.Random

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

  override def pprSingleTarget(seedPersonIds: util.List[Integer],
                               targetPersonId: Int,
                               params: BidirectionalPPRParams): Double = {
    val estimator = new BidirectionalPPREstimator(graph, params.resetProbability.toFloat)
    val startDistribution = new UniformDistribution(CollectionUtil.toScala(seedPersonIds), new Random())
    val minimumPPR = if (params.isSetMinProbability)
      params.minProbability.toFloat
    else
      0.25f / graph.maxNodeId // nodeCount would be more natural but isn't available for GraphUnion

    // TODO: Modify estimator to incorporate maxIntermediateNodeId if it ever becomes an issue
    val estimate = estimator.estimatePPR(
      startDistribution,
      targetPersonId,
      minimumPPR,
      params.relativeError.toFloat)
    if (estimate >= minimumPPR)
      estimate
    else
      0.0 // Return a clean 0.0 rather than noise if PPR value is too small
  }
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
