package co.teapot.tempest.server

import java.util

import co.teapot.tempest.util.ConfigLoader
import co.teapot.tempest.{MonteCarloPageRankParams, Node}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._


class TempestDBServerSpec extends FlatSpec with Matchers {
  def make_server(): TempestDBServer = {
    val configFileName = "src/test/resources/config/tempest.yaml"
    val config = ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    val dbConfigPath = "src/test/resources/config/database.yaml"
    val dbConfig = ConfigLoader.loadConfig[DatabaseConfig](dbConfigPath)
    val databaseClient: TempestDatabaseClient = new TempestSQLDatabaseClient(dbConfig)
    new TempestDBServer( databaseClient, config)
  }

  "A TempestDB server" should "work on PPR calls" in {
    val server = make_server()
    val aliceNode = new Node("user", "alice")
    server.outNeighbors("has_read", aliceNode).asScala should contain theSameElementsAs Seq(
      new Node("book", "101"),
      new Node("book", "103"))

    val prParams = new MonteCarloPageRankParams(1000, 0.3)
    val seeds = Seq(aliceNode).asJava
    val pprMap = server.pprUndirected(util.Arrays.asList("has_read"), seeds, prParams).asScala
    println(s"pprMap: $pprMap")
    pprMap(new Node("user", "alice")) should be > pprMap(new Node("user", "bob"))
    pprMap(new Node("book", "101")) should be > pprMap(new Node("book", "103"))
    pprMap(new Node("book", "103")) should be > pprMap(new Node("book", "102"))

    // Note: Many more tempest calls are tested in TempestDBServerClientSpec
    // Going forward, tests for new calls can go here or there (or both!)
  }
}
