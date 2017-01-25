package co.teapot.tempest.server

import java.io.File
import co.teapot.tempest.{SQLException, UndefinedAttributeException}
import co.teapot.tempest.util.ConfigLoader
import org.scalatest.{Matchers, FlatSpec}
import anorm.{SqlParser, SQL}

class TempestSQLDatabaseClientSpec extends FlatSpec with Matchers {
  // TODO: Fix tests!
  /*
  val testConfigPath = "src/test/resources/config/database.yml"
  if (new File(testConfigPath).exists()) {
    val testConfig = ConfigLoader.loadConfig[DatabaseConfig](testConfigPath)
    "A TempestDatabaseClientSpec" should "connect correctly" in {
      val c = new TempestSQLDatabaseClient(testConfig)
      c.getSingleTypeNodeAttributeAsJSON("graph1", Seq(1, 3), "name").toSeq should contain theSameElementsAs
        Seq(1L -> "\"Alice Johnson\"", 3L -> "\"Carol \"ninja\" Coder\"")
      c.getNodeAttributeAsJSON("graph1", 1L, "login_count") shouldEqual ("5")
      c.getNodeAttributeAsJSON("graph1", 1L, "premium_subscriber") shouldEqual ("false")
      c.getNodeAttributeAsJSON("graph1", 3L, "premium_subscriber") shouldEqual ("true")

      // node id 4 has null name
      c.getNodeAttributeAsJSON("graph1", 4L, "name") shouldEqual "null"

      // TODO: UndefinedAttributeException would be more precise than SQLException
      an[SQLException] should be thrownBy {
        c.getMultiNodeAttribute[String]("graph1", Seq(1L, 3L), "invalidAttributeName")
      }

      c.getNodeIdsWithAttributeValue("graph1", "username", "alice") should contain theSameElementsAs (Seq(1L))

      c.nodeIdsMatchingClause("graph1", "login_count > 2") should contain theSameElementsAs (Seq(1L, 3L))

      c.filterNodeIdsUsingAttributeValue("graph1", Seq(1L, 2L), "username", "alice") should contain theSameElementsAs (Seq(1L))
      c.filterNodeIds("graph1", Seq(2L, 3L), "login_count > 2") should contain theSameElementsAs (Seq(3L))

      c.filterNodeIds("graph1", Seq(1L, 2L), "name = 'Alice Johnson'") should contain theSameElementsAs (Seq(1L))

      a [SQLException] should be thrownBy {
        c.nodeIdsMatchingClause("graph1", "Bizzare * %")
      }


      // Test node update
      c.setNodeAttribute("graph1", 2L, "username", "newBob")
      c.getNodeAttribute[String]("graph1", 2L, "username") should equal (Some("newBob"))
      c.setNodeAttribute("graph1", 2L, "username", "bob")

      // TODO: Eventually add and test support for adding new nodes
      //c.withConnection { implicit connection =>
      //  SQL("DELETE FROM graph1_nodes WHERE id = 42").execute()
      //}

      // Test edges creation TODO
      //c.addEdges("graph1", Array(10L, 20L), Array(11L, 21L))
      //c.withConnection { implicit connection =>
      //  SQL("select id2 from edges WHERE id1 = 10").as(SqlParser.int(1).*) should contain (11)
      //  SQL("select id1 from edges WHERE id2 = 21").as(SqlParser.int(1).*) should contain (20)
      //  SQL("DELETE FROM edges WHERE id2 in (20, 21)").execute()
      //}

      // Test graph2 returns independent results
      c.getNodeAttributeAsJSON("graph2", 1L, "username") shouldEqual "\"alice2\""
      c.getNodeIdsWithAttributeValue("graph2", "username", "alice2") should contain theSameElementsAs (Seq(1L))
      c.getNodeIdsWithAttributeValue("graph2", "username", "alice") should contain theSameElementsAs (Seq())
      c.getNodeAttributeAsJSON("graph2", 1L, "login_count2") shouldEqual "52"
      c.nodeIdsMatchingClause("graph2", "login_count2 = 32") should contain theSameElementsAs (Seq(3L))
      c.nodeIdsMatchingClause("graph2", "username = 'nameless'") should contain theSameElementsAs (Seq(3000000004L))

    }
  } else {
    System.err.println(s"Omitting database tests due to missing config file $testConfigPath")
  }
  */
}
