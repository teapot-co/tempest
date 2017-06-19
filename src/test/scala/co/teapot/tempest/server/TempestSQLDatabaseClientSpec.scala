package co.teapot.tempest.server

import co.teapot.tempest.typedgraph.Node
import co.teapot.tempest.{SQLException, InvalidArgumentException, Node => ThriftNode}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, Suite}

trait H2DatabaseBasedTest {
  this: Suite =>
  val testConfig: DatabaseConfig = new H2DatabaseConfig

  protected def createTempestSQLDatabaseClient(): TempestSQLDatabaseClient = {
    new TempestSQLDatabaseClient(testConfig)
  }
}

trait SyntheticDatabaseData extends BeforeAndAfterEach {
  this: H2DatabaseBasedTest with Suite =>

  def prepareTables(connection: java.sql.Connection): Unit = {
    // TODO: Perhaps we should load this directly from the config/csv files
    val sql =
      """
        |DROP TABLE IF EXISTS user_nodes;
        |CREATE TABLE user_nodes (
        |    tempest_id SERIAL PRIMARY KEY,
        |    name varchar,
        |    id varchar UNIQUE NOT NULL,
        |    login_count int,
        |    premium_subscriber boolean
        |);
        |
        |DROP TABLE IF EXISTS book_nodes;
        |CREATE TABLE book_nodes (
        |    tempest_id SERIAL PRIMARY KEY,
        |    id varchar UNIQUE NOT NULL,
        |    title varchar
        |);
        |
        |INSERT INTO user_nodes (name, id, login_count, premium_subscriber) VALUES ('Alice Johnson','alice',5,false);
        |INSERT INTO user_nodes (name, id, login_count, premium_subscriber) VALUES ('Bob Smith, Jr.','bob',2,false);
        |INSERT INTO user_nodes (name, id, login_count, premium_subscriber) VALUES ('Carol "ninja" Coder','carol',3,true);
        |INSERT INTO user_nodes (name, id, login_count, premium_subscriber) VALUES (null,'nameless',0,false);
        |INSERT INTO user_nodes (name, id, login_count, premium_subscriber) VALUES ('sneaky','; DROP TABLE user;',0,true);
        |
        |INSERT INTO book_nodes (id, title) VALUES ('101','The Lord of the Rings');
        |INSERT INTO book_nodes (id, title) VALUES ('102','The Grapes of Wrath');
        |INSERT INTO book_nodes (id, title) VALUES ('103','Roots');
        |
      """.stripMargin

    val stmt = connection.createStatement()
    stmt.execute(sql)
  }

  override def beforeEach() {
    val config = createTempestSQLDatabaseClient()
    val connection = config.connectionSource.getConnection

    prepareTables(connection)

    super.beforeEach() // To be stackable, must call super.beforeEach
  }

  override def afterEach(): Unit = {
    val config = createTempestSQLDatabaseClient()
    val connection = config.connectionSource.getConnection
    val stmt = connection.createStatement()
    stmt.execute("DROP ALL OBJECTS DELETE FILES")
    super.afterEach()
  }
}

class TempestSQLDatabaseClientSpec extends FlatSpec with Matchers with H2DatabaseBasedTest with SyntheticDatabaseData {


  "A TempestDatabaseClientSpec" should "connect correctly" in {
    val c = createTempestSQLDatabaseClient()

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
    c.toNode(new ThriftNode("user", "alice")) shouldEqual (Node("user", 1))
    c.nodeToThriftNodeMap(Seq(Node("user", 1), Node("user", 3))) should contain theSameElementsAs
      Map(Node("user", 1) -> new ThriftNode("user", "alice"),
        Node("user", 3) -> new ThriftNode("user", "carol"))

    c.nodeIdsMatchingClause("user", "login_count > 2") should contain theSameElementsAs (Seq("alice", "carol"))

    c.filterNodeIds("user", Seq("bob", "carol"), "login_count > 2") should contain theSameElementsAs (Seq("carol"))

    c.filterNodeIds("user", Seq("alice", "bob"), "name = 'Alice Johnson'") should contain theSameElementsAs (Seq("alice"))

    a[SQLException] should be thrownBy {
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
    c.thriftNodeToNodeMap(Seq(new ThriftNode("book", "101"), new ThriftNode("book", "103"))) should contain theSameElementsAs
      Map(new ThriftNode("book", "101") -> Node("book", 1),
        new ThriftNode("book", "103") -> Node("book", 3))
  }

  it should "accept arbitrary non-existent ids" in {
    val c = createTempestSQLDatabaseClient()
    // use an id that simulates SQL injection attack and test that such queries do not crash
    val nonexistentId = Seq("; DROP TABLE user")
    c.getSingleTypeNodeAttributeAsJSON("user", nonexistentId, "name").toSeq should contain theSameElementsAs
      Seq.empty
    c.filterNodeIds("user", nonexistentId, "name = 'Alice Johnson'") should contain theSameElementsAs Seq.empty
    c.getMultiNodeAttribute[String]("user", nonexistentId, "name") should contain theSameElementsAs Seq.empty
  }

  it should "find arbitrary ids" in {
    val c = createTempestSQLDatabaseClient()
    val nodeId = "; DROP TABLE user;"
    c.getSingleTypeNodeAttributeAsJSON("user", Seq(nodeId), "name").toSeq should contain theSameElementsAs {
      Seq("; DROP TABLE user;" -> "\"sneaky\"")
    }
    c.filterNodeIds("user", Seq(nodeId), "name = 'sneaky'") should contain theSameElementsAs {
      Seq(nodeId)
    }
    c.getMultiNodeAttribute[String]("user", Seq(nodeId), "name") should contain theSameElementsAs {
      Map(nodeId -> "sneaky")
    }
  }

  it should "limit number of ids passed" in {
    val c = createTempestSQLDatabaseClient()
    val nodeIds = (1 to 100000).map(_.toString)
    an[InvalidArgumentException] should be thrownBy {
      c.getSingleTypeNodeAttributeAsJSON("user", nodeIds, "name")
    }
  }

  it should "add new nodes" in {
    val c = createTempestSQLDatabaseClient()
    val nodeType = "user"

    {
      val nodeIds = (1 to 10).map(_.toString)
      val nodes = nodeIds.map { id => new ThriftNode(nodeType, id) }
      c.addNewNodes(nodes)
    }

    {
      val nodeIds = (5 to 15).map(_.toString)
      val nodes = nodeIds.map { id => new ThriftNode(nodeType, id) }
      c.addNewNodes(nodes)
    }

    val allNodeIds = (1 to 15).map(_.toString)

    c.filterNodeIds(nodeType, allNodeIds, "1 = 1") should contain theSameElementsAs {
      allNodeIds
    }

    {
      val nodeIds = (1 to 10).map(_.toString)
      val nodes = nodeIds.map { id => new ThriftNode(nodeType, id) }
      an[SQLException] should be thrownBy {
        c.addNodes(nodes)
      }
    }
  }

}
