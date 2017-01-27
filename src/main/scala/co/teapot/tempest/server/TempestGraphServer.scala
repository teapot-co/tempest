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
import co.teapot.tempest.util.{ConfigLoader, LogUtil}
import co.teapot.thriftbase.TeapotThriftLauncher
import org.apache.thrift.TProcessor
import soal.ppr.BidirectionalPPREstimator
import soal.util.UniformDistribution

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestGraphServer(databaseClient: TempestDatabaseClient, config: TempestDBServerConfig)
    extends TempestGraphService.Iface {
  val graphMap = new mutable.HashMap[String, DynamicDirectedGraph]()
  def graph(edgeType: String): DynamicDirectedGraph = {
    if (! graphMap.contains(edgeType)) {
      val graphFile = new File(config.graphDirectory, s"$edgeType.dat")
      if (graphFile.exists) {
        val graph = new MemMappedDynamicDirectedGraph(
          graphFile,
          syncAllWrites = false /* Graph persistence is handled by database.*/)
        graphMap(edgeType) = graph
      } else {
        throw new InvalidArgumentException(s"Invalid edge type $edgeType")
      }
    }
    graphMap(edgeType)
  }

  def loadEdgeConfig(edgeType: String): EdgeTypeConfig = {
    val edgeConfigFile = new File(config.graphConfigDirectoryFile, s"$edgeType.yaml")
    if (! edgeConfigFile.exists()) {
      throw new UndefinedGraphException(s"Graph config file ${edgeConfigFile.getCanonicalPath} not found")
    }
    ConfigLoader.loadConfig[EdgeTypeConfig](edgeConfigFile)
  }

  /** Returns the type of node reached by following the given direction (out-neighbors or in-neighbors) on the given
    * edge type.
    */
  def edgeEndpointType(edgeType: String, direction: EdgeDir): String = {
    val edgeConfig = loadEdgeConfig(edgeType)
    direction match {
      case EdgeDirOut => edgeConfig.targetNodeType
      case EdgeDirIn => edgeConfig.sourceNodeType
    }
  }

  def doesNodeTypeHaveAttribute(nodeType: String, attributeName: String): Boolean = {
    true // TODO
  }

  override def outDegree(edgeType: String, node: Node): Int = {
    val tempestId = databaseClient.nodeToTempestId(node)
    graph(edgeType).outDegree(tempestId)
  }

  override def inDegree(edgeType: String, node: Node): Int = {
    val tempestId = databaseClient.nodeToTempestId(node)
    graph(edgeType).inDegree(tempestId)
  }

  def neighbors(edgeType: String, node: Node, direction: EdgeDir): util.List[Node] = {
    val tempestId = databaseClient.nodeToTempestId(node)
    val resultType = edgeEndpointType(edgeType, direction)
    val neighborTempestIds = graph(edgeType).neighbors(tempestId, direction)
    databaseClient.tempestIdToNodeMulti(resultType, neighborTempestIds).asJava
  }

  override def outNeighbors(edgeType: String, node: Node): util.List[Node] =
    neighbors(edgeType, node, EdgeDirOut)

  override def inNeighbors(edgeType: String, node: Node): util.List[Node] =
    neighbors(edgeType, node, EdgeDirIn)

  def neighbor(edgeType: String, node: Node, i: Int, direction: EdgeDir): Node = {
    val tempestId = databaseClient.nodeToTempestId(node)
    val degree = graph(edgeType).degree(tempestId, direction)
    if (i >= degree)
      throw new InvalidIndexException(s"Invalid index $i for node $node with degree $degree")

    val resultType = edgeEndpointType(edgeType, direction)
    val neighborTempestId = graph(edgeType).neighbor(tempestId, i, direction)
    databaseClient.tempestIdToNode(resultType, neighborTempestId)
  }

  override def outNeighbor(edgeType: String, node: Node, i: Int): Node =
    neighbor(edgeType, node, i, EdgeDirOut)

  override def inNeighbor(edgeType: String, node: Node, i: Int): Node =
    neighbor(edgeType, node, i, EdgeDirIn)

  override def edgeCount(edgeType: String): Long = graph(edgeType).edgeCount

  override def nodeCount(edgeType: String): Int = graph(edgeType).nodeCountOption.getOrElse(
    throw new InvalidArgumentException("nodeCount not supported on this graph type"))

  override def maxNodeId(edgeType: String): Int = graph(edgeType).maxNodeId

  override def pprSingleTarget(edgeType: String, seedNodesJava: util.List[Node],
                               targetNode: Node,
                               params: BidirectionalPPRParams): Double = {
    val seedIntNodes = databaseClient.nodeToIntNodeMap(seedNodesJava.asScala).values

    val expectedSeedType = loadEdgeConfig(edgeType).sourceNodeType
    for (u <- seedIntNodes) {
      if (u.`type` != expectedSeedType)
        throw new InvalidArgumentException(s"Invalid seed type ${u.`type`} for edge type $edgeType")
    }

    val seedTempestIds = seedIntNodes map (_.tempestId)
    val targetTempestId = databaseClient.nodeToTempestId(targetNode)
    validateBidirectionalPPRParams(params)

    val estimator = new BidirectionalPPREstimator(graph(edgeType), params.resetProbability.toFloat)
    val startDistribution = new UniformDistribution(seedTempestIds.toArray[Int], new Random())
    val minimumPPR = if (params.isSetMinProbability)
      params.minProbability.toFloat
    else
      0.25f / graph(edgeType).maxNodeId // nodeCount would be more natural but isn't available for GraphUnion

    // TODO: Modify estimator to incorporate maxIntermediateNodeId if it ever becomes an issue
    val estimate = estimator.estimatePPR(
      startDistribution,
      targetTempestId,
      minimumPPR,
      params.relativeError.toFloat)
    if (estimate >= minimumPPR)
      estimate
    else
      0.0 // Return a clean 0.0 rather than noise if PPR value is too small
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
  /* TODO: Restore this after merging TempestGraphServer with TempestServer
  def getProcessor(graphDirectory: String): TProcessor = {
    val server = new TempestGraphServer(new File(graphDirectory))
    new TempestGraphService.Processor(server)
  }

  def main(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args, getProcessor, "")
  }
  */
}
