package co.teapot.tempest.server

import co.teapot.tempest.MonteCarloPageRankParams
import co.teapot.tempest.util.{CollectionUtil, ConfigLoader}
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConverters._


class TempestDBServerSpec extends FlatSpec with Matchers {
  def make_server(): TempestDBServer = {
    val configFileName = "src/test/resources/config/tempest.yaml"
    val config = ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    val databaseClient: TempestDatabaseClient = null // TODO: Mock DB? = new TempestSQLDatabaseClient(databaseConfig)
    new TempestDBServer( databaseClient, config)
  }
  /* TODO
  "A TempestDB server" should "work on PPR calls" in {
    val server = make_server()
    server.outNeighbors("has_read", 1) should contain theSameElementsAs Seq(101, 103)

    val prParams = new MonteCarloPageRankParams(1000, 0.3)
    val seeds = CollectionUtil.toJava(Seq(1L))
    val pprToBooks = server.ppr("has_read", seeds, "user", "book", prParams)
    pprToBooks.keySet() should contain theSameElementsAs Seq(101, 102, 103)
    pprToBooks.asScala.maxBy(_._2)._1 shouldEqual 101

    val pprToUser = server.ppr("has_read", seeds, "user", "user", prParams)
    pprToUser.keySet() should contain theSameElementsAs Seq(1L, 2L, 3L)
    pprToUser.asScala.maxBy(_._2)._1 shouldEqual 1

    val pprUserFollowsRight = server.ppr("follows", seeds, "left", "right", prParams)
    pprUserFollowsRight.keySet() should contain theSameElementsAs Seq(2L)

    val pprUserFollowsLeft = server.ppr("follows", seeds, "left", "left", prParams)
    pprUserFollowsLeft.keySet() should contain theSameElementsAs Seq(1L, 3L)

    val pprUserFollowsAny = server.ppr("follows", seeds, "left", "any", prParams)
    pprUserFollowsAny.keySet() should contain theSameElementsAs Seq(1L, 2L, 3L)
    pprUserFollowsAny.get(1L).doubleValue should be > 0.1

    // Test that non-alternating walk only follows out-edges
    prParams.alternatingWalk = false
    prParams.resetProbability = 0.00001
    val pprUserFollowsAnyNonAlternating = server.ppr("follows", seeds, "left", "any", prParams)
    pprUserFollowsAnyNonAlternating.get(1L).doubleValue should be < 0.01
  }
  */
}
