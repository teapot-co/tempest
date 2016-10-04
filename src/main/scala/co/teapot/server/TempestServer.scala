/*
 * Copyright 2016 Teapot, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package co.teapot.server

import java.util

import co.teapot.graph._
import co.teapot.thriftbase.TeapotThriftLauncher
import co.teapot.util.LogUtil
import co.teapot.util.CollectionUtil
import org.apache.thrift.TProcessor
import soal.ppr.BidirectionalPPREstimator
import soal.util.UniformDistribution

import scala.util.Random

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestServer(graph: DirectedGraph) extends TempestService.Iface {
  override def outDegree(id: Int): Int = {
    validateNodeId(id)
    graph.outDegree(id)
  }

  override def inDegree(id: Int): Int = {
    validateNodeId(id)
    graph.inDegree(id)
  }

  override def outNeighbors(id: Int): util.List[Integer] = {
    validateNodeId(id)
    graph.outNeighborList(id)
  }

  override def inNeighbors(id: Int): util.List[Integer] = {
    validateNodeId(id)
    graph.inNeighborList(id)
  }

  override def outNeighbor(id: Int, i: Int): Int = {
    validateNodeId(id)
    if (i >= graph.outDegree(id))
      throw new InvalidIndexException(s"Invalid index for node $id with out-degree ${graph.outDegree(id)}")
    graph.outNeighbor(id, i)
  }

  override def inNeighbor(id: Int, i: Int): Int = {
    validateNodeId(id)
    if (i >= graph.inDegree(id))
      throw new InvalidIndexException(s"Invalid index for node $id with in-degree ${graph.inDegree(id)}")
    graph.inNeighbor(id, i)
  }

  override def edgeCount(): Long = graph.edgeCount

  override def nodeCount(): Int = graph.nodeCountOption.getOrElse(
    throw new InvalidArgumentException("nodeCount not supported on this graph type"))

  override def maxNodeId(): Int = graph.maxNodeId

  override def pprSingleTarget(seedPersonIdsJava: util.List[Integer],
                               targetPersonId: Int,
                               params: BidirectionalPPRParams): Double = {
    val seedPersonIds = CollectionUtil.integersToScala(seedPersonIdsJava)
    for (id <- seedPersonIds)
      validateNodeId(id)
    validateNodeId(targetPersonId)
    validateBidirectionalPPRParams(params)

    val estimator = new BidirectionalPPREstimator(graph, params.resetProbability.toFloat)
    val startDistribution = new UniformDistribution(seedPersonIds, new Random())
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

  def validateNodeId(id: Int): Unit = {
    if (!graph.existsNode(id)) {
      throw new InvalidNodeIdException(s"Invalid node id $id")
    }
  }

  def validateBidirectionalPPRParams(params: BidirectionalPPRParams): Unit = {
    if (params.resetProbability >= 1.0 || params.resetProbability <= 0.0) {
      throw new InvalidArgumentException("resetProbability must be between 0.0 and 1.0")
    }
    if (params.relativeError <= 0.0) {
      throw new InvalidArgumentException("relativeError must be positive")
    }
    if (params.isSetMinProbability && params.minProbability <= 0.0) {
      throw new InvalidArgumentException("minProbability must be positive")
    }
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
