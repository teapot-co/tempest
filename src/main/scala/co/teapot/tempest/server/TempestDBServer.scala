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
import java.{lang, util}

import co.teapot.tempest.{Node => ThriftNode, _}
import co.teapot.tempest.algorithm.MonteCarloPPRTyped
import co.teapot.tempest.graph._
import co.teapot.tempest.typedgraph.{BipartiteTypedGraph, Node, TypedGraphUnion}
import co.teapot.tempest.util.{CollectionUtil, ConfigLoader, LogUtil}
import co.teapot.thriftbase.TeapotThriftLauncher
import org.apache.thrift.TProcessor
import soal.ppr.BidirectionalPPREstimator
import soal.util.UniformDistribution

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestDBServer(databaseClient: TempestDatabaseClient, config: TempestDBServerConfig)
    extends TempestDBService.Iface {

  // Load Graphs
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

  def typedGraph(edgeType: String): BipartiteTypedGraph = {
    val edgeConfig = loadEdgeConfig(edgeType)
    BipartiteTypedGraph(edgeConfig.sourceNodeType, edgeConfig.targetNodeType, graph(edgeType))
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



  override def outDegree(edgeType: String, thriftNode: ThriftNode): Int = {
    val tempestId = databaseClient.toNode(thriftNode).tempestId
    graph(edgeType).outDegree(tempestId)
  }

  override def inDegree(edgeType: String, thriftNode: ThriftNode): Int = {
    val tempestId = databaseClient.toNode(thriftNode).tempestId
    graph(edgeType).inDegree(tempestId)
  }

  def neighbors(edgeType: String, thriftNode: ThriftNode, direction: EdgeDir): util.List[ThriftNode] = {
    val tempestId = databaseClient.toNode(thriftNode).tempestId
    val resultType = edgeEndpointType(edgeType, direction)
    val neighborTempestIds = graph(edgeType).neighbors(tempestId, direction)
    databaseClient.tempestIdToThriftNodeMulti(resultType, neighborTempestIds).asJava
  }

  override def outNeighbors(edgeType: String, thriftNode: ThriftNode): util.List[ThriftNode] =
    neighbors(edgeType, thriftNode, EdgeDirOut)

  override def inNeighbors(edgeType: String, thriftNode: ThriftNode): util.List[ThriftNode] =
    neighbors(edgeType, thriftNode, EdgeDirIn)

  def neighbor(edgeType: String, thriftNode: ThriftNode, i: Int, direction: EdgeDir): ThriftNode = {
    val tempestId = databaseClient.toNode(thriftNode).tempestId
    val degree = graph(edgeType).degree(tempestId, direction)
    if (i >= degree)
      throw new InvalidIndexException(s"Invalid index $i for node $thriftNode with degree $degree")

    val resultType = edgeEndpointType(edgeType, direction)
    val neighborTempestId = graph(edgeType).neighbor(tempestId, i, direction)
    databaseClient.toThriftNode(Node(resultType, neighborTempestId))
  }

  override def outNeighbor(edgeType: String, node: ThriftNode, i: Int): ThriftNode =
    neighbor(edgeType, node, i, EdgeDirOut)

  override def inNeighbor(edgeType: String, node: ThriftNode, i: Int): ThriftNode =
    neighbor(edgeType, node, i, EdgeDirIn)



  override def connectedComponent(sourceNode: ThriftNode, edgeTypes: util.List[String], maxSize: Int): util.List[ThriftNode] = {
    val source = databaseClient.toNode(sourceNode)

    val typedGraphs = edgeTypes.asScala map typedGraph
    val unionGraph = new TypedGraphUnion(typedGraphs)

    val reachedNodes = new mutable.HashSet[Node]()
    val nodesToVisit = new util.ArrayDeque[Node]()
    reachedNodes += source
    nodesToVisit.push(source)
    while (! nodesToVisit.isEmpty && reachedNodes.size < maxSize) {
      val u = nodesToVisit.pop()
      for (v <- unionGraph.neighbors(u)) {
        if (! reachedNodes.contains(v) && reachedNodes.size < maxSize) {
          reachedNodes += v
          nodesToVisit.push(v)
        }
      }
    }
    val resultNodes = databaseClient.nodeToThriftNodeMap(reachedNodes).values
    new util.ArrayList(resultNodes.asJavaCollection)
  }

  type DegreeFilter = collection.Map[DegreeFilterTypes, Int]

  /** Returns the type of node reached after k steps along the given edge type starting with the given
    * initial direction. If the edge type
    * is from sourceNodeType to targetNodeType, and edgeDir is EdgeDirOut, this will return sourceNodeType if k is even,
    * or targetNodeType if
    * k is odd.  The parity is swapped if edgeDir is EdgeDirIn.
    */
  def kStepNodeType(edgeType: String, edgeDir: EdgeDir, k: Int): String = {
    val edgeConfig = loadEdgeConfig(edgeType)
    if ((edgeDir == EdgeDirOut && k % 2 == 0) ||
        (edgeDir == EdgeDirIn  && k % 2 == 1)) {
      edgeConfig.getSourceNodeType
    } else {
      edgeConfig.getTargetNodeType
    }
  }

  def satisfiesFilters(edgeType: String, nodeId: Long, degreeFilter: DegreeFilter): Boolean =
    degreeFilter.forall {case(filterType, filterValue) =>
      satisfiesSingleFilter(edgeType, nodeId, filterType, filterValue)
    }

  def satisfiesSingleFilter(edgeType: String, nodeId: Long, filterType: DegreeFilterTypes, filterValue: Int): Boolean =
    filterType match { // TODO: Support multiple graphs here
      case DegreeFilterTypes.INDEGREE_MAX => graph(edgeType).inDegree(nodeId.toInt) <= filterValue
      case DegreeFilterTypes.INDEGREE_MIN => graph(edgeType).inDegree(nodeId.toInt) >= filterValue
      case DegreeFilterTypes.OUTDEGREE_MAX => graph(edgeType).outDegree(nodeId.toInt) <= filterValue
      case DegreeFilterTypes.OUTDEGREE_MIN => graph(edgeType).outDegree(nodeId.toInt) >= filterValue
      case default => true
    }


  def kStepNeighborsFiltered(edgeType: String,
                             source: ThriftNode,
                             k: Int,
                             sqlClause: String,
                             edgeDir: EdgeDir,
                             degreeFilter: DegreeFilter,
                             alternating: Boolean): util.List[ThriftNode] = {
    val sourceTempestId = databaseClient.toNode(source).tempestId
    val targetNodeType = kStepNodeType(edgeType, edgeDir, k)
    val effectiveGraph = edgeDir match {
      case EdgeDirOut => graph(edgeType)
      case EdgeDirIn => graph(edgeType).transposeView
    }
    val neighborhood = DirectedGraphAlgorithms.kStepOutNeighbors(effectiveGraph, sourceTempestId, k, alternating).toIntArray
    val resultPreFilter: Seq[Int] = if (sqlClause.isEmpty || neighborhood.isEmpty) {
      neighborhood
    } else {
      if (neighborhood.size < TempestServerConstants.MaxNeighborhoodAttributeQuerySize) {
        databaseClient.tempestIdsMatchingClause(targetNodeType, sqlClause + " AND tempest_id in " + neighborhood.mkString("(", ",", ")"))
      } else {
        val candidates = databaseClient.tempestIdsMatchingClause(targetNodeType, sqlClause)
        candidates filter neighborhood.contains
      }
    }
    val resultTempestIds = resultPreFilter filter { id => satisfiesFilters(edgeType, id, degreeFilter) }
    val resultNodes = databaseClient.tempestIdToThriftNodeMulti(targetNodeType, resultTempestIds)
    resultNodes.asJava
  }

  override def kStepOutNeighborsFiltered(edgeType: String,
                                         source: ThriftNode,
                                         k: Int,
                                         sqlClause: String,
                                         filter: java.util.Map[DegreeFilterTypes, Integer],
                                         alternating: Boolean): util.List[ThriftNode] =
    kStepNeighborsFiltered(edgeType, source, k, sqlClause, EdgeDirOut,
                                       CollectionUtil.toScala(filter), alternating)


  override def kStepInNeighborsFiltered(edgeType: String,
                                        source: ThriftNode,
                                        k: Int,
                                        sqlClause: String,
                                        filter: java.util.Map[DegreeFilterTypes, Integer],
                                        alternating: Boolean): util.List[ThriftNode] =
    kStepNeighborsFiltered(edgeType, source, k, sqlClause, EdgeDirIn,
                           CollectionUtil.toScala(filter), alternating)

  def validateMonteCarloParams(params: MonteCarloPageRankParams): Unit = {
    if (params.resetProbability >= 1.0 || params.resetProbability <= 0.0) {
      throw new InvalidArgumentException("resetProbability must be between 0.0 and 1.0")
    }
    if (params.numSteps <= 0) {
      throw new InvalidArgumentException("numSteps must be positive")
    }
    if (params.isSetMaxResultCount && params.maxResultCount <= 0) {
      throw new InvalidArgumentException("maxResultCount must be positive")
    }
  }

  override def pprUndirected(edgeTypes: util.List[String],
                             seedNodesJava: util.List[ThriftNode],
                             pageRankParams: MonteCarloPageRankParams): util.Map[ThriftNode, lang.Double] = {
    validateMonteCarloParams(pageRankParams)
    val seedNodes = seedNodesJava.asScala
    val seeds = databaseClient.thriftNodeToNodeMap(seedNodes).values.toIndexedSeq

    val typedGraphs = edgeTypes.asScala map typedGraph
    val unionGraph = new TypedGraphUnion(typedGraphs)
    val pprMap = MonteCarloPPRTyped.estimatePPR(unionGraph, seeds, pageRankParams)
    val intNodeToNodeMap = databaseClient.nodeToThriftNodeMap(pprMap.keys)
    (pprMap map { case (intNode, value) =>
      (intNodeToNodeMap(intNode), new lang.Double(value))
    }).asJava
  }


  override def pprSingleTarget(edgeType: String, seedNodesJava: util.List[ThriftNode],
                               targetThriftNode: ThriftNode,
                               params: BidirectionalPPRParams): Double = {
    val seedIntNodes = databaseClient.thriftNodeToNodeMap(seedNodesJava.asScala).values

    val expectedSeedType = loadEdgeConfig(edgeType).sourceNodeType
    for (u <- seedIntNodes) {
      if (u.`type` != expectedSeedType)
        throw new InvalidArgumentException(s"Invalid seed type ${u.`type`} for edge type $edgeType")
    }

    val seedTempestIds = seedIntNodes map (_.tempestId)
    val targetTempestId = databaseClient.toNode(targetThriftNode).tempestId
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



  override def nodeCount(edgeType: String): Int = graph(edgeType).nodeCountOption.getOrElse(
    throw new InvalidArgumentException("nodeCount not supported on this graph type"))

  override def edgeCount(edgeType: String): Long = graph(edgeType).edgeCount

  def nodes(nodeType: String, sqlClause: String): util.List[ThriftNode] = {
    val nodeIds = databaseClient.nodeIdsMatchingClause(nodeType, sqlClause)
    (nodeIds map { id => new ThriftNode(nodeType, id)}).asJava
  }

  override def getMultiNodeAttributeAsJSON(nodesJava: util.List[ThriftNode], attributeName: String): util.Map[ThriftNode, String] = {
    val nodeTypes = (nodesJava.asScala map (_.`type`)).toSet
    for (nodeType <- nodeTypes) {
      if (! doesNodeTypeHaveAttribute(nodeType, attributeName)) {
        throw new InvalidArgumentException(s"Node type $nodeType does not have an attribute named $attributeName")
      }
    }
    databaseClient.getMultiNodeAttributeAsJSON(nodesJava.asScala, attributeName).asJava
  }



  def setNodeAttribute(node: ThriftNode, attributeName: String, attributeValue: String): Unit =
    databaseClient.setNodeAttribute(node, attributeName, attributeValue)

  override def addEdges(edgeType: String,
                        sourceNodesJava: util.List[ThriftNode],
                        targetNodesJava: util.List[ThriftNode]): Unit = {
    if (sourceNodesJava.size != targetNodesJava.size) {
      throw new UnequalListSizeException()
    }
    val sourceNodes = sourceNodesJava.asScala
    val targetNodes = targetNodesJava.asScala

    val nodeToIntNodeMap = databaseClient.thriftNodeToNodeMap(sourceNodes ++ targetNodes)
    val sourceTempestIds = sourceNodes map { node: ThriftNode => nodeToIntNodeMap(node).tempestId }
    val targetTempestIds = targetNodes map { node: ThriftNode => nodeToIntNodeMap(node).tempestId }

    // TODO: Add edges to DB?
    // databaseClient.addEdges(graphName: String, CollectionUtil.toScala(sourceIds), CollectionUtil.toScala(destinationIds))
    for ((sourceId, targetId) <- sourceTempestIds zip targetTempestIds) {
      graph(edgeType).addEdge(sourceId, targetId) // Future optimization: efficient Multi-add
    }
  }
}



object TempestDBServer {
  def getProcessor(configFileName: String): TProcessor = {
    val config = ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    // Not currently used: ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    val databaseConfigFile = "/root/tempest/system/database.yaml" // TODO: move db config to main config?
    val databaseConfig = ConfigLoader.loadConfig[DatabaseConfig](databaseConfigFile)
    val databaseClient = new TempestSQLDatabaseClient(databaseConfig)

    val server = new TempestDBServer(databaseClient, config)
    new TempestDBService.Processor(server)
  }

  def main(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args, getProcessor, "/root/tempest/system/tempest.yaml")
  }
}

object TempestServerConstants {
  // Note: This should be moved to a config file.
  val MaxNeighborhoodAttributeQuerySize = 1000 * 1000
}
