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

import java.{lang, util}

import co.teapot.algorithm.MonteCarloPPR
import co.teapot.graph.EdgeDir.EdgeDir
import co.teapot.graph._
import co.teapot.tempest._
import co.teapot.thriftbase.TeapotThriftLauncher
import co.teapot.util.{CollectionUtil, ConfigLoader, LogUtil}
import org.apache.thrift.TProcessor

import scala.collection.JavaConverters._

/** Given a graph, this thrift server responds to requests about that graph. */
class TempestDBServer(graph: DynamicDirectedGraph, databaseClient: TempestDatabaseClient, config: TempestDBServerConfig)
    extends TempestServer(graph) with TempestDBService.Iface {

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

  def neighborsWithinKStepsFiltered(graphName: String,
                                    sourceId: Long,
                                    k: Int,
                                    sqlClause: String,
                                    edgeDir: EdgeDir,
                                    degreeFilter: DegreeFilter): util.List[lang.Long] = {
    validateNodeId(sourceId)

    if (k > 3) // TODO (?): Limit the output size instead of limiting the distance
      throw new InvalidArgumentException("For efficiency, this call is limited to 4 steps from the source")
    val effectiveGraph = edgeDir match {
      case EdgeDir.Out => graph
      case EdgeDir.In => graph.transposeView
    }
    val neighborhoodInts = DirectedGraphAlgorithms.outNeighborsWithinKSteps(effectiveGraph, sourceId.toInt, k)
    val neighborhood = neighborhoodInts.toIntArray map Integer.toUnsignedLong
    val resultPreFilter: Seq[Long] = if (sqlClause.isEmpty) {
      neighborhood
    } else {
      if (neighborhood.size < TempestServerConstants.MaxNeighborhoodAttributeQuerySize) {
        databaseClient.filterNodeIds(graphName, neighborhood, sqlClause)
      } else {
        val candidates = databaseClient.nodeIdsFiltered(graphName, sqlClause)
        candidates filter neighborhood.contains
      }
    }
    CollectionUtil.toJava(resultPreFilter filter { id => satisfiesFilters(graphName, id, degreeFilter) })
  }

  override def outNeighborsWithinKStepsFiltered(graphName: String,
                                                sourceId: Long,
                                                k: Int,
                                                sqlClause: String,
                                                filter: java.util.Map[DegreeFilterTypes, Integer]): util.List[lang.Long] =
    neighborsWithinKStepsFiltered(graphName, sourceId, k, sqlClause, EdgeDir.Out,
                                       CollectionUtil.toScala(filter))


  override def inNeighborsWithinKStepsFiltered(graphName: String,
                                               sourceId: Long,
                                               k: Int,
                                               sqlClause: String,
                                               filter: java.util.Map[DegreeFilterTypes, Integer]): util.List[lang.Long] =
    neighborsWithinKStepsFiltered(graphName, sourceId, k, sqlClause, EdgeDir.In,
                                       CollectionUtil.toScala(filter))

  override def ppr(graphName: String,
                   seedsJava: util.List[lang.Long],
                   pageRankParams: MonteCarloPageRankParams): util.Map[lang.Long, lang.Double] = {
    validateMonteCarloParams(pageRankParams)
    val seeds = CollectionUtil.toScala(seedsJava)
    for (id <- seeds)
      validateNodeId(id)
    // TODO: We should use graphName once we store multiple graphs.  For now, just use global graph.
    CollectionUtil.toJava(MonteCarloPPR.estimatePPR(graph,
      seeds map { _.toInt },
      pageRankParams) map {case (id, ppr) => (Integer.toUnsignedLong(id), ppr) })
  }

  def satisfiesFilters(graphName: String, nodeId: Long, degreeFilter: DegreeFilter): Boolean =
    degreeFilter.forall {case(filterType, filterValue) =>
      satisfiesSingleFilter(graphName, nodeId, filterType, filterValue)
    }

  def satisfiesSingleFilter(graphName: String, nodeId: Long, filterType: DegreeFilterTypes, filterValue: Int): Boolean =
    filterType match { // TODO: Support multiple graphs here
      case DegreeFilterTypes.INDEGREE_MAX => graph.inDegree(nodeId.toInt) <= filterValue
      case DegreeFilterTypes.INDEGREE_MIN => graph.inDegree(nodeId.toInt) >= filterValue
      case DegreeFilterTypes.OUTDEGREE_MAX => graph.outDegree(nodeId.toInt) <= filterValue
      case DegreeFilterTypes.OUTDEGREE_MIN => graph.outDegree(nodeId.toInt) >= filterValue
      case default => true
    }

  def validateNodeId(id: Long): Unit =
    super.validateNodeId(id.toInt)

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

  override def addEdges(graphName: String,
                        sourceIds: util.List[lang.Long],
                        destinationIds: util.List[lang.Long]): Unit = {
    if (sourceIds.size != destinationIds.size) {
      throw new UnequalListSizeException()
    }
    //// TODO: Multiple graphs databaseClient.addEdges(graphName: String, CollectionUtil.toScala(sourceIds), CollectionUtil.toScala(destinationIds))
    for ((srcId, destId) <- sourceIds.asScala zip destinationIds.asScala) {
      // TODO: Multiple graphs
      graph.addEdge(srcId.toInt, destId.toInt) // TODO: More efficient Multi-add
    }
  }
}

object TempestDBServer {
  def getProcessor(configFileName: String): TProcessor = {
    val config = new TempestDBServerConfig()
    // Not currently used: ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    val databaseConfigFile = "config/database.yaml"
    val databaseConfig = ConfigLoader.loadConfig[DatabaseConfig](databaseConfigFile)
    val databaseClient = new TempestSQLDatabaseClient(databaseConfig)

    // TODO: Restore: val binaryGraphFilename = s"data/${GRAPH}_mapped_edges.dat"
    //val graph = new MemMappedDynamicDirectedGraph(
    //  new File(binaryGraphFilename),
    //  syncAllWrites = false /* TODO: revert to true if this doesn't work.*/)
    val graph = DynamicDirectedGraph()
    val server = new TempestDBServer(graph, databaseClient, config)
    new TempestDBService.Processor(server)
  }

  def main(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args, getProcessor, "")
  }
}

/** Launches a server using small graph and test database. */
object TempestDBServerTest {
  val databaseConfig = ConfigLoader.loadConfig[DatabaseConfig]("src/test/resources/config/database.yml")
  def getProcessor(configFileName: String): TProcessor = {
    val config = new TempestDBServerConfig() // ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    val databaseClient = new TempestSQLDatabaseClient(databaseConfig)

    val graph = DynamicDirectedGraph(1 -> 2, 2 -> 3)

    val server = new TempestDBServer(graph, databaseClient, config)
    new TempestDBService.Processor(server)
  }

  def launch(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args, getProcessor, "config/tempest.yaml")
  }

  def main(args: Array[String]): Unit = launch(args)
}

object TempestServerConstants {
  val MaxNeighborhoodAttributeQuerySize =  1000 * 1000
}
