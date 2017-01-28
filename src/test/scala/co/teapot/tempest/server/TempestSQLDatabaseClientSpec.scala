package co.teapot.tempest.server

import co.teapot.tempest.typedgraph.IntNode
import co.teapot.tempest.util.ConfigLoader
import co.teapot.tempest.{Node => ThriftNode, SQLException}
import com.zaxxer.hikari.pool.HikariPool.PoolInitializationException
import org.scalatest.{FlatSpec, Matchers}

class TempestSQLDatabaseClientSpec extends FlatSpec with Matchers {
  val testConfigPath = "src/test/resources/config/database.yaml"
  val testConfig = ConfigLoader.loadConfig[DatabaseConfig](testConfigPath)

  "A TempestDatabaseClientSpec" should "connect correctly" in {
    val c: TempestSQLDatabaseClient = try {
      new TempestSQLDatabaseClient(testConfig)
    } catch {
      case e: PoolInitializationException =>
        throw new RuntimeException("Failed to connect to postgres.\nYou may want to follow the directions in Readme.md " +
          "on setting up a dev docker instance.\nIn particular, make sure you used -p 5432:5432 when starting docker" +
          "and docker is running.\nYou may need to run /root/tempest/system/start_postgres.sh inside docker.")
    }


    c.getSingleTypeNodeAttributeAsJSON("user", Seq("alice", "carol"), "name").toSeq should contain theSameElementsAs
      Seq("alice" -> "\"Alice Johnson\"", "carol" -> "\"Carol \"ninja\" Coder\"")
    c.getNodeAttributeAsJSON("user", "alice", "login_count") shouldEqual ("5")
    c.getNodeAttributeAsJSON("user", "alice", "premium_subscriber") shouldEqual ("false")
    c.getNodeAttributeAsJSON("user", "carol", "premium_subscriber") shouldEqual ("true")

    // node "nameless" has null name
    c.getNodeAttributeAsJSON("user", "nameless", "name") shouldEqual "null"

    // TODO: UndefinedAttributeException would be more precise than SQLException
    an[SQLException] should be thrownBy {
      c.getMultiNodeAttribute[String]("user", Seq("alice", "carol"), "invalidAttributeName")
    }

    c.getTempestIdsWithAttributeValue("user", "name", "Alice Johnson") should contain theSameElementsAs (Seq(1))
    c.nodeToIntNode(new ThriftNode("user", "alice")) shouldEqual (IntNode("user", 1))
    c.intNodeToNodeMap(Seq(IntNode("user", 1), IntNode("user", 3))) should contain theSameElementsAs
      Map(IntNode("user", 1) -> new ThriftNode("user", "alice"),
        IntNode("user", 3) -> new ThriftNode("user", "carol"))

    c.nodeIdsMatchingClause("user", "login_count > 2") should contain theSameElementsAs (Seq("alice", "carol"))

    c.filterNodeIds("user", Seq("bob", "carol"), "login_count > 2") should contain theSameElementsAs (Seq("carol"))

    c.filterNodeIds("user", Seq("alice", "bob"), "name = 'Alice Johnson'") should contain theSameElementsAs (Seq("alice"))

    a [SQLException] should be thrownBy {
      c.nodeIdsMatchingClause("user", "Bizzare * %")
    }


    // Test node update
    // TODO: Restore after implementing addNode
    //c.setNodeAttribute("graph1", 2L, "username", "newBob")
    //c.getNodeAttribute[String]("graph1", 2L, "username") should equal (Some("newBob"))
    //c.setNodeAttribute("graph1", 2L, "username", "bob")

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

    // Test book node type returns independent results
    c.getNodeAttributeAsJSON("book", "103", "title") shouldEqual "\"Roots\""
    c.nodeIdsMatchingClause("book", "title = 'Roots'") should contain theSameElementsAs (Seq("103"))
    c.getTempestIdsWithAttributeValue("book", "title", "Roots") should contain theSameElementsAs (Seq(3))
    c.nodeToIntNodeMap(Seq(new ThriftNode("book", "101") , new ThriftNode("book", "103") )) should contain theSameElementsAs
      Map(new ThriftNode("book", "101") -> IntNode("book", 1),
        new ThriftNode("book", "103") -> IntNode("book", 3))
  }
}
