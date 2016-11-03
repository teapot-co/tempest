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

package co.teapot.tempest.server

import java.io.File
import java.util

import co.teapot.tempest._
import co.teapot.tempest.graph._
import co.teapot.thriftbase.TeapotThriftLauncher
import co.teapot.tempest.util.LogUtil
import co.teapot.tempest.util.CollectionUtil
import org.apache.thrift.TProcessor
import soal.ppr.BidirectionalPPREstimator
import soal.util.UniformDistribution

import scala.collection.mutable
import scala.util.Random

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestGraphServer(graphDirectory: File) extends TempestGraphService.Iface {
  val graphMap = new mutable.HashMap[String, DynamicDirectedGraph]()
  def graph(edgeType: String): DynamicDirectedGraph = {
    if (! graphMap.contains(edgeType)) {
      val graphFile = new File(graphDirectory, edgeType)
      if (graphFile.exists) {
        val graph = new MemMappedDynamicDirectedGraph(
          graphFile,
          syncAllWrites = false /* Graph persistence is handled by database.*/)
        graphMap(edgeType) = graph
      } else {
        throw new InvalidArgumentException("Invalid edge type \"$edgeType\"")
      }
    }
    graphMap(edgeType)
  }

  override def outDegree(edgeType: String, id: Int): Int = {
    validateNodeId(edgeType, id)
    graph(edgeType).outDegree(id)
  }

  override def inDegree(edgeType: String, id: Int): Int = {
    validateNodeId(edgeType, id)
    graph(edgeType).inDegree(id)
  }

  override def outNeighbors(edgeType: String, id: Int): util.List[Integer] = {
    validateNodeId(edgeType, id)
    graph(edgeType).outNeighborList(id)
  }

  override def inNeighbors(edgeType: String, id: Int): util.List[Integer] = {
    validateNodeId(edgeType, id)
    graph(edgeType).inNeighborList(id)
  }

  override def outNeighbor(edgeType: String, id: Int, i: Int): Int = {
    validateNodeId(edgeType, id)
    if (i >= graph(edgeType).outDegree(id))
      throw new InvalidIndexException(s"Invalid index $i for node $id with out-degree ${graph(edgeType).outDegree(id)}")
    graph(edgeType).outNeighbor(id, i)
  }

  override def inNeighbor(edgeType: String, id: Int, i: Int): Int = {
    validateNodeId(edgeType, id)
    if (i >= graph(edgeType).inDegree(id))
      throw new InvalidIndexException(s"Invalid index $i for node $id with in-degree ${graph(edgeType).inDegree(id)}")
    graph(edgeType).inNeighbor(id, i)
  }

  override def edgeCount(edgeType: String): Long = graph(edgeType).edgeCount

  override def nodeCount(edgeType: String): Int = graph(edgeType).nodeCountOption.getOrElse(
    throw new InvalidArgumentException("nodeCount not supported on this graph type"))

  override def maxNodeId(edgeType: String): Int = graph(edgeType).maxNodeId

  override def pprSingleTarget(edgeType: String, seedPersonIdsJava: util.List[Integer],
                               targetPersonId: Int,
                               params: BidirectionalPPRParams): Double = {
    val seedPersonIds = CollectionUtil.integersToScala(seedPersonIdsJava)
    for (id <- seedPersonIds)
      validateNodeId(edgeType, id)
    validateNodeId(edgeType, targetPersonId)
    validateBidirectionalPPRParams(params)

    val estimator = new BidirectionalPPREstimator(graph(edgeType), params.resetProbability.toFloat)
    val startDistribution = new UniformDistribution(seedPersonIds, new Random())
    val minimumPPR = if (params.isSetMinProbability)
      params.minProbability.toFloat
    else
      0.25f / graph(edgeType).maxNodeId // nodeCount would be more natural but isn't available for GraphUnion

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

  def validateNodeId(edgeType: String, id: Int): Unit = {
    if (!graph(edgeType).existsNode(id)) {
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

object TempestGraphServer {
  def getProcessor(graphDirectory: String): TProcessor = {
    val server = new TempestGraphServer(new File(graphDirectory))
    new TempestGraphService.Processor(server)
  }

  def main(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args, getProcessor, "")
  }
}
