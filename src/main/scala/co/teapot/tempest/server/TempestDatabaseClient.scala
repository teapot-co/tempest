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

import java.sql.{Connection, PreparedStatement, Types, BatchUpdateException}

import anorm._
import co.teapot.tempest.{Node => ThriftNode, _}
import co.teapot.tempest.typedgraph.Node
import org.postgresql.util.PSQLException

import scala.collection.mutable
import scala.collection.immutable

/**
  * A TempestDatabaseClient provides methods to query attributes of nodes, find nodes matching a given query, and
  * convert between ThriftNode objects (which have a string id chosen by the user) and internal Node objects
  * (which have an Int tempestId assigned by tempest). */

trait TempestDatabaseClient {
  def toNode(node: ThriftNode): Node = {
    toNodeOption(node).getOrElse(
      throw new InvalidNodeIdException(s"No node in ${node.`type`} has id ${node.id}")
    )
  }

  def toNodeOption(node: ThriftNode): Option[Node] =
    thriftNodeToNodeMap(Seq(node)).get(node)

  def thriftNodeToNodeMap(sourceNodes: Iterable[ThriftNode]): collection.Map[ThriftNode, Node] = {
    val result = new mutable.AnyRefMap[ThriftNode, Node]()
    val nodesByType = sourceNodes groupBy { node => node.`type` }
    for ((nodeType, nodes) <- nodesByType) {
      val ids = nodes map (_.id)
      val idToTempestIdString = getSingleTypeNodeAttributeAsJSON(nodeType, ids.toSeq, "tempest_id")
      for (node <- nodes) {
        val tempestId = idToTempestIdString.getOrElse(node.id,
          throw new InvalidNodeIdException(s"Invalid node $node")
        ).toInt
        result(node) = Node(nodeType, tempestId)
      }
    }
    result
  }

  def tempestIdToIdPairMulti(nodeType: String, tempestIds: Iterable[Int]): List[(Int, String)]

  def tempestIdToThriftNodeMulti(nodeType: String, tempestIds: Seq[Int]): Seq[ThriftNode] = {
    if (tempestIds.isEmpty)
      return Seq.empty
    val ids = nodeIdsMatchingClause(nodeType, "tempest_id in " + tempestIds.mkString("(", ",", ")"))
    ids map { id =>
      new ThriftNode(nodeType, id)
    }
  }

  def toThriftNode(node: Node): ThriftNode = {
    tempestIdToThriftNodeMulti(node.`type`, Seq(node.tempestId)).head
  }

  def nodeToThriftNodeMap(intNodes: Iterable[Node]): collection.Map[Node, ThriftNode] = {
    val result = new mutable.AnyRefMap[Node, ThriftNode]()
    val intNodesByType = intNodes groupBy { node => node.`type` }
    for ((nodeType, intNodes) <- intNodesByType) {
      val tempestIds = intNodes map (_.tempestId)
      val tempestIdToIdMap = tempestIdToIdPairMulti(nodeType, tempestIds)
      for ((tempestId, id) <- tempestIdToIdMap) {
        result(Node(nodeType, tempestId)) = new ThriftNode(nodeType, id)
      }
    }
    result
  }

  def getNodeAttributeAsJSON(nodeType: String, nodeId: String, attributeName: String): String =
    getSingleTypeNodeAttributeAsJSON(nodeType, Seq(nodeId), attributeName).getOrElse(nodeId, "null")

  def getSingleTypeNodeAttributeAsJSON(nodeType: String, nodeIds: Seq[String], attributeName: String): collection.Map[String, String]

  def getMultiNodeAttributeAsJSON(nodes: Seq[ThriftNode], attributeName: String): collection.Map[ThriftNode, String] = {
    val nodeByType = nodes groupBy { node => node.`type` }
    val nodeToAttribute = new mutable.HashMap[ThriftNode, String]()
    for ((nodeType, nodes) <- nodeByType) {
      val ids = nodes map (_.id)
      val idToJson = getSingleTypeNodeAttributeAsJSON(nodeType, ids, attributeName)
      for ((id, value) <- idToJson) {
        nodeToAttribute(new ThriftNode(nodeType, id)) = value
      }
    }
    nodeToAttribute
  }

  def addNode(node: ThriftNode): Unit

  def addNodes(nodes: Seq[ThriftNode]): Unit

  def addNewNodes(nodes: Seq[ThriftNode]): Unit

  def setNodeAttribute(node: ThriftNode,
                       attributeName: String,
                       attributeValue: String): Unit

  def getTempestIdsWithAttributeValue(nodeType: String,
                                      attributeName: String,
                                      attributeValue: String): Seq[Int]

  def filterNodeIds(nodeType: String, nodeIds: Seq[String], sqlClause: String): Seq[String]

  def tempestIdsMatchingClause(nodeType: String, sqlClause: String): Seq[Int]

  def nodeIdsMatchingClause(nodeType: String, sqlClause: String): Seq[String]

  def addEdges(nodeType: String, ids1: Seq[String], ids2: Seq[String]): Unit
}

class TempestSQLDatabaseClient(config: DatabaseConfig) extends TempestDatabaseClient {
  val connectionSource = config.createConnectionSource()

  def nodesTable(nodeType: String): String =
    nodeType + "_nodes"

  def withConnection[A](body: Connection => A): A = {
    implicit val connection = connectionSource.getConnection()
    try {
      body(connection)
    } catch {
      case e: PSQLException => throw new SQLException(e.getMessage)
    }
    finally {
      connection.close()
    }
  }

  def idToTempestId(node: ThriftNode): Option[Int] =
    withConnection { implicit connection =>
      val statement =
        SQL(s"SELECT tempest_id FROM ${nodesTable(node.`type`)} WHERE id = {id}").on("id" -> node.id)
      statement.as(SqlParser.int("tempest_id").singleOpt)
    }

  def tempestIdToIdPairMulti(nodeType: String, tempestIds: Iterable[Int]): List[(Int, String)] =
    withConnection { implicit connection =>
      if (tempestIds.isEmpty)
        return List.empty

      val statement =
        SQL(s"SELECT tempest_id, id FROM ${nodesTable(nodeType)} WHERE tempest_id in (${tempestIds.mkString(",")})")
      val rowParser = SqlParser.int("tempest_id") ~ SqlParser.str("id") map { case x ~ y => (x, y) }
      statement.as(rowParser.*)
    }

  def getMultiNodeAttribute[A](nodeType: String, nodeIds: Seq[String], attributeName: String)
                              (implicit c: anorm.Column[A]): collection.Map[String, A] =
    withConnection { implicit connection =>
      if (nodeIds.isEmpty)
        return Map.empty
      validateAttributeName(attributeName)
      validateNodeIds(nodeIds)

      val statement =
        s"SELECT id, $attributeName FROM ${nodesTable(nodeType)} WHERE id IN ({nodeIds})"
      // apply() here is deprecated, but it's easier to use than its replacements
      val idValuePairs = SQL(statement).on('nodeIds -> nodeIds).apply().flatMap { row =>
        // Use flatMap and Option to omit null values
        row[Option[A]](attributeName) map { value: A =>
          (row[String]("id"), value)
        }
      }
      idValuePairs.toMap
    }

  def getSingleTypeNodeAttributeAsJSON(nodeType: String,
                                       nodeIds: Seq[String],
                                       attributeName: String): collection.Map[String, String] =
    withConnection { implicit connection =>
      if (nodeIds.isEmpty)
        return Map.empty
      validateAttributeName(attributeName)


      // generates (?, ?, ..., ?), one question mark per node id
      val sqlInParams = Iterator.fill(nodeIds.size)("?").mkString("(", ",", ")")
      val sql =
        s"SELECT id, $attributeName FROM ${nodesTable(nodeType)} WHERE id in $sqlInParams"
      val result = new mutable.HashMap[String, String]()

      // Use JDBC directly rather than ANorm to access column types
      val pstmt: PreparedStatement = connection.prepareStatement(sql)
      // SQL indexes parameters from 1 instead of 0
      for ((id, i) <- nodeIds.zipWithIndex)
        pstmt.setString(i + 1, id)

      val resultSet = pstmt.executeQuery()
      val resultType = resultSet.getMetaData.getColumnType(2)
      while (resultSet.next()) {
        val id = resultSet.getString(1)
        val valueJSON: String = resultType match {
          case Types.INTEGER => resultSet.getInt(2).toString
          case Types.BIGINT => resultSet.getString(2)
          case Types.VARCHAR => "\"" + resultSet.getString(2) + "\""
          case Types.BOOLEAN | Types.BIT => resultSet.getBoolean(2).toString
          case unexpectedCode: Int => throw new SQLException(s"Unknown column type code $unexpectedCode from DB")
        }
        if (resultSet.wasNull) {
          result(id) = "null"
        } else {
          result(id) = valueJSON
        }
      }
      result
    }

  def addNode(node: ThriftNode): Unit =
    withConnection { implicit connection =>
      SQL(s"INSERT INTO ${nodesTable(node.`type`)} (id) VALUES ({id})")
        .on("id" -> node.id).execute()
    }

  def addNodes(nodes: Seq[ThriftNode]): Unit = {
    if (nodes.isEmpty)
      return

    try {
      // Currently, only support adding nodes of 1 type in a batch
      val nodeTypes = nodes.map(_.`type`).toSet

      if (nodeTypes.size > 1)
        throw new InvalidArgumentException(s"All nodes need to be of the same type. Encountered node types: $nodeTypes")

      val nodeIds = nodes.map(_.id).distinct
      val tableName = nodesTable(nodes.head.`type`)

      withConnection { implicit connection =>
        val parameters = nodeIds.map { id => Seq[NamedParameter]("id" -> id) }

        // Separating into head and tail is weird, but for some reason the anorm library wants it
        // this way
        val query = BatchSql(s"INSERT INTO $tableName (id) VALUES({id})",
          parameters.head, parameters.tail:_*
        )

        query.execute()
      }
    } catch {
      case e: BatchUpdateException => throw new SQLException(e.getMessage)
    }
  }

  def addNewNodes(nodes: Seq[ThriftNode]): Unit = {
    if (nodes.isEmpty)
      return

    val nodeIds = nodes.map(_.id)
    val nodeType = nodes.head.`type`

    val existingNodeIds = filterNodeIds(nodeType, nodeIds, "1 = 1").toSet
    val newNodes = nodes.filterNot { node => existingNodeIds.contains(node.id) }

    addNodes(newNodes)
  }

  def setNodeAttribute(node: ThriftNode, attributeName: String, attributeValue: String): Unit =
    withConnection { implicit connection =>
      validateAttributeName(attributeName)
      // Note, we quote/escape attributeValue to prevent SQL injection, but can't quote attributeName
      // because its a column name.
      SQL(s"UPDATE ${nodesTable(node.`type`)} SET $attributeName = {attributeValue} WHERE id = {id}")
        .on("attributeValue" -> attributeValue, "id" -> node.id).execute()
    }

  def getTempestIdsWithAttributeValue(nodeType: String, attributeName: String,
                                      attributeValue: String): Seq[Int] =
    withConnection { implicit connection =>
      validateAttributeName(attributeName)
      SQL(s"SELECT tempest_id FROM ${nodesTable(nodeType)} WHERE $attributeName = {attributeValue}")
        .on("attributeValue" -> attributeValue)
        .as(SqlParser.int(1).*)
    }

  def tempestIdsMatchingClause(nodeType: String, sqlClause: String): Seq[Int] =
    withConnection { implicit connection =>
      rejectUnsafeSQL(sqlClause)
      SQL(s"SELECT tempest_id FROM ${nodesTable(nodeType)} WHERE $sqlClause")
        .as(SqlParser.int(1).*)
    }

  def nodeIdsMatchingClause(nodeType: String, sqlClause: String): Seq[String] =
    withConnection { implicit connection =>
      rejectUnsafeSQL(sqlClause)
      SQL(s"SELECT id FROM ${nodesTable(nodeType)} WHERE $sqlClause")
        .as(SqlParser.str(1).*)
    }

  /** Returns all node ids in the given Seq which have the given attribute value. */
  def filterNodeIds(nodeType: String, nodeIds: Seq[String], sqlClause: String): Seq[String] =
    withConnection { implicit connection =>
      if (nodeIds.isEmpty)
        return Seq.empty
      validateNodeIds(nodeIds)
      rejectUnsafeSQL(sqlClause)
      val tableName = nodesTable(nodeType)
      val query = SQL(s"SELECT id FROM $tableName WHERE $sqlClause AND id IN ({nodeIds})").on('nodeIds -> nodeIds)
      query.as(SqlParser.str(1).*)
    }

  def addEdges(edgeType: String, ids1: Seq[String], ids2: Seq[String]): Unit = ???

  // TODO: Revive this in the future if we want to store new edges in the DB
  /*
  withConnection { implicit connection =>
    val idList = ids1.mkString("(", ",", ")")
    val sql =
      s"SELECT id, tempest_id FROM ${nodesTable(nodeType)} WHERE id IN $idList"
    val edgesString = (tempestIds1 zip tempestIds2).mkString(", ")
    SQL(s"INSERT INTO edges (id1, id2) VALUES $edgesString").executeUpdate()
  }
}
*/

  def rejectUnsafeSQL(sqlClause: String): Unit = {
    // TODO: Improve SQL injection defense
    if (sqlClause.contains(";"))
      throw new InvalidArgumentException(s"Malformed sql clause: '$sqlClause'")
  }

  def validateNodeIds(ids: Seq[String]): Unit = {
    val maxIdSize = 10000
    if (ids.size > maxIdSize)
      throw new InvalidArgumentException(s"The number ${ids.size} of node ids exceeds the limit of $maxIdSize." +
        s"The limit prevents SQL query size explosion.")
  }

  def stringsToPostgresSet(strings: Iterable[String]): String =
    strings.mkString("('", "','", "')")

  /** For security/safety, make sure attributeName is a valid identifier */
  def validateAttributeName(attributeName: String): Unit = {
    if (!attributeName.matches("[a-zA-Z0-9_]*"))
      throw new InvalidArgumentException(s"Invalid attribute name '$attributeName'")
  }
}
