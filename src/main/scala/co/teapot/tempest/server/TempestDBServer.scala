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

import co.teapot.tempest._
import co.teapot.tempest.algorithm.MonteCarloPPR
import co.teapot.tempest.graph.EdgeDir.EdgeDir
import co.teapot.tempest.graph.{DirectedGraphAlgorithms, DynamicDirectedGraph, EdgeDir, MemMappedDynamicDirectedGraph}
import co.teapot.tempest.util.{CollectionUtil, ConfigLoader, LogUtil}
import co.teapot.thriftbase.TeapotThriftLauncher
import org.apache.thrift.TProcessor

import scala.collection.JavaConverters._

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestDBServer(databaseClient: TempestDatabaseClient, config: TempestDBServerConfig)
    extends TempestGraphServer(config.graphDirectoryFile) with TempestDBService.Iface {

  type DegreeFilter = collection.Map[DegreeFilterTypes, Int]
  override def getMultiNodeAttributeAsJSON(graphName: String, nodeIdsJava: util.List[lang.Long], attributeName: String): util.Map[lang.Long, String] = {
    val nodeIds = CollectionUtil.toScala(nodeIdsJava)
    val idToJson = databaseClient.getMultiNodeAttributeAsJSON(graphName, nodeIds, attributeName)
    CollectionUtil.toJavaLongStringMap(idToJson)
  }

  def setNodeAttribute(graphName: String, nodeId: Long, attributeName: String, attributeValue: String): Unit =
    databaseClient.setNodeAttribute(graphName, nodeId, attributeName, attributeValue)

  def nodes(graphName: String, sqlClause: String): util.List[lang.Long] =
    CollectionUtil.toJava(databaseClient.nodeIdsFiltered(graphName, sqlClause))

  /** Returns the type of node reached after k steps along the given edge type. If the edge type
    * is from nodeType1 to nodeType2, this will return nodeType1 if k is even, or nodeType2 if
    * k is odd.
    */
  def kStepNodeType(edgeType: String, edgeDir: EdgeDir, k: Int): String = {
    val edgeConfigFile = new File(config.graphConfigDirectoryFile, s"$edgeType.yaml")
    if (! edgeConfigFile.exists()) {
      throw new UndefinedGraphException(s"Graph config file ${edgeConfigFile.getCanonicalPath} not found")
    }
    val edgeConfig = ConfigLoader.loadConfig[EdgeTypeConfig](edgeConfigFile)

    if ((edgeDir == EdgeDir.Out && k % 2 == 0) ||
        (edgeDir == EdgeDir.In  && k % 2 == 1)) {
      edgeConfig.getNodeType1
    } else {
      edgeConfig.getNodeType2
    }
  }

  def kStepNeighborsFiltered(edgeType: String,
                             sourceId: Long,
                             k: Int,
                             sqlClause: String,
                             edgeDir: EdgeDir,
                             degreeFilter: DegreeFilter,
                             alternating: Boolean): util.List[lang.Long] = {
    validateNodeId(edgeType, sourceId)
    val targetNodeType = kStepNodeType(edgeType, edgeDir, k)
    val effectiveGraph = edgeDir match {
      case EdgeDir.Out => graph(edgeType)
      case EdgeDir.In => graph(edgeType).transposeView
    }
    val neighborhoodInts = DirectedGraphAlgorithms.kStepOutNeighbors(effectiveGraph, sourceId.toInt, k, alternating)
    val neighborhood = neighborhoodInts.toIntArray map Integer.toUnsignedLong
    val resultPreFilter: Seq[Long] = if (sqlClause.isEmpty) {
      neighborhood
    } else {
      if (neighborhood.size < TempestServerConstants.MaxNeighborhoodAttributeQuerySize) {
        databaseClient.filterNodeIds(targetNodeType, neighborhood, sqlClause)
      } else {
        val candidates = databaseClient.nodeIdsFiltered(targetNodeType, sqlClause)
        candidates filter neighborhood.contains
      }
    }
    CollectionUtil.toJava(resultPreFilter filter { id => satisfiesFilters(edgeType, id, degreeFilter) })
  }

  override def kStepOutNeighborsFiltered(edgeType: String,
                                         sourceId: Long,
                                         k: Int,
                                         sqlClause: String,
                                         filter: java.util.Map[DegreeFilterTypes, Integer],
                                         alternating: Boolean): util.List[lang.Long] =
    kStepNeighborsFiltered(edgeType, sourceId, k, sqlClause, EdgeDir.Out,
                                       CollectionUtil.toScala(filter), alternating)


  override def kStepInNeighborsFiltered(edgeType: String,
                                        sourceId: Long,
                                        k: Int,
                                        sqlClause: String,
                                        filter: java.util.Map[DegreeFilterTypes, Integer],
                                        alternating: Boolean): util.List[lang.Long] =
    kStepNeighborsFiltered(edgeType, sourceId, k, sqlClause, EdgeDir.In,
                           CollectionUtil.toScala(filter), alternating)

  override def ppr(edgeType: String,
                   seedsJava: util.List[lang.Long],
                   pageRankParams: MonteCarloPageRankParams): util.Map[lang.Long, lang.Double] = {
    validateMonteCarloParams(pageRankParams)
    val seeds = CollectionUtil.toScala(seedsJava)
    for (id <- seeds)
      validateNodeId(edgeType, id)
    CollectionUtil.toJava(MonteCarloPPR.estimatePPR(graph(edgeType),
      seeds map { _.toInt },
      pageRankParams) map {case (id, ppr) => (Integer.toUnsignedLong(id), ppr) })
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

  def validateNodeId(edgeType: String, id: Long): Unit =
    super.validateNodeId(edgeType, id.toInt)

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

  override def addEdges(edgeType: String,
                        sourceIds: util.List[lang.Long],
                        destinationIds: util.List[lang.Long]): Unit = {
    if (sourceIds.size != destinationIds.size) {
      throw new UnequalListSizeException()
    }
    // TODO: Add edges to DB?
    // databaseClient.addEdges(graphName: String, CollectionUtil.toScala(sourceIds), CollectionUtil.toScala(destinationIds))
    for ((srcId, destId) <- sourceIds.asScala zip destinationIds.asScala) {
      graph(edgeType).addEdge(srcId.toInt, destId.toInt) // Future optimization: efficient Multi-add
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
  val MaxNeighborhoodAttributeQuerySize = 1000 * 1000
}
