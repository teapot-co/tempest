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

import java.sql.{Connection, Types}

import anorm._
import co.teapot.tempest.{InvalidArgumentException, SQLException, UndefinedAttributeException}
import org.postgresql.util.PSQLException

import scala.collection.mutable

trait TempestDatabaseClient {
  /** Returns a map from node id to attribute value for the the given attribute name.  Any nodes
    * with null attribute value will be omitted from the resulting map.
    * The implicit anorm.Column[A] parameter just means that A must be some type which Anorm
    * can extract from a JDBC row. */
  def getMultiNodeAttribute[A](graphName: String, nodeIds: Seq[Long], attributeName: String)
                              (implicit c : anorm.Column[A]): collection.Map[Long, A]  // TODO: Delete method

  def getMultiNodeAttributeAsJSON(graphName: String, nodeIds: Seq[Long], attributeName: String): collection.Map[Long, String]

  def getNodeAttribute[A](graphName: String, nodeId: Long, attributeName: String)
                         (implicit c : anorm.Column[A]): Option[A] =
    getMultiNodeAttribute(graphName, Seq(nodeId), attributeName).get(nodeId)

  def getNodeAttributeAsJSON(graphName: String, nodeId: Long, attributeName: String): String =
    getMultiNodeAttributeAsJSON(graphName, Seq(nodeId), attributeName).getOrElse(nodeId, "null")

  /*  If we create a mock database, we might need to go back to these methods:
    def getNodeStringAttribute(nodeId: Long, attributeName: String): String =
    getNodeAttribute(nodeId, attributeName, SqlParser.str(1))
  def getNodeLongAttribute(nodeId: Long, attributeName: String): Long =
    getNodeAttribute(nodeId, attributeName, SqlParser.long(1))
  def getNodeBooleanAttribute(nodeId: Long, attributeName: String): Boolean =
    getNodeAttribute(nodeId, attributeName, SqlParser.bool(1))
   */
  def setNodeAttribute(graphName: String,
                       nodeId: Long,
                       attributeName: String,
                       attributeValue: String): Unit

  def getNodeIdsWithAttributeValue(graphName: String,
                                   attributeName: String,
                                   attributeValue: String): Seq[Long]

  def filterNodeIdsUsingAttributeValue(graphName: String,
                                       nodeIds: Seq[Long],
                                       attributeName: String,
                                       attributeValue: String): Seq[Long]

  def filterNodeIds(graphName: String, nodeIds: Seq[Long], sqlClause: String): Seq[Long]

  def nodeIdsFiltered(graphName: String, sqlClause: String): Seq[Long]

  def addEdges(graphName: String, ids1: Seq[Long], ids2: Seq[Long]): Unit
}

class TempestSQLDatabaseClient(config: DatabaseConfig) extends TempestDatabaseClient {
  val connectionSource = config.createConnectionSource()

  val attributeColumns = new mutable.HashMap[String, collection.Set[String]]()

  def nodesTable(graphName: String): String =
    graphName + "_nodes"

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

  def getMultiNodeAttribute[A](graphName: String, nodeIds: Seq[Long], attributeName: String)
                              (implicit c : anorm.Column[A]): collection.Map[Long, A] =
    withConnection { implicit connection =>
      validateAttributeName(graphName, attributeName)
      val idList = nodeIds.mkString("(", ",", ")")
      val statement =
        s"SELECT id, $attributeName FROM ${nodesTable(graphName)} WHERE id IN $idList"
      // apply() here is deprecated, but it's easier to use than its replacements
      val idValuePairs = SQL(statement).apply().flatMap { row =>
        // Use flatMap and Option to omit null values
        row[Option[A]](attributeName) map { value: A =>
          (row[Long]("id"), value)
        }
      }
      idValuePairs.toMap
    }

  def getMultiNodeAttributeAsJSON(graphName: String,
                                nodeIds: Seq[Long],
                                attributeName: String): collection.Map[Long, String] =
    withConnection { implicit connection =>
      // For security/safety, make sure attributeName is a valid identifier
      if (! attributeName.matches("[a-zA-Z0-9_]*"))
        throw new UndefinedAttributeException (s"Invalid attribute name '$attributeName'")
      val idList = nodeIds.mkString("(", ",", ")")
      val sql =
        s"SELECT id, $attributeName FROM ${nodesTable(graphName)} WHERE id IN $idList"
      val result = new mutable.HashMap[Long, String]()

      // Use JDBC directly rather than ANorm to access column types
      val s = connection.createStatement()

      val resultSet = s.executeQuery(sql)
      val resultType = resultSet.getMetaData.getColumnType(2)
      while (resultSet.next()) {
        val id = resultSet.getLong(1)
        val valueJSON: String = resultType match {
          case Types.INTEGER => resultSet.getInt(2).toString
          case Types.BIGINT => resultSet.getLong(2).toString
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


  def setNodeAttribute(graphName: String, nodeId: Long, attributeName: String, attributeValue: String): Unit =
    withConnection { implicit connection =>
      validateAttributeName(graphName, attributeName)
      //if (attributeColumns.contains(attributeName)) {
        // Note, we quote/escape attributeValue to prevent SQL injection, but can't quote attributeName
        // because its a column name.
        SQL(s"UPDATE ${nodesTable(graphName)} SET $attributeName = {attributeValue} WHERE id = $nodeId")
          .on("attributeValue" -> attributeValue).execute()
      //} else {
      //  ??? // TODO: Support ad-hoc attributes
        // Example: UPDATE ${nodesTable(graphName)} SET json_attributes=jsonb_set(json_attributes, '{favorite_color}', '"Royal Blue"', true) where id=2;
      //}
    }

  def getNodeIdsWithAttributeValue(graphName: String, attributeName: String,
                                   attributeValue: String): Seq[Long] =
    withConnection { implicit connection =>
      validateAttributeName(graphName, attributeName)
      SQL(s"SELECT id FROM ${nodesTable(graphName)} WHERE $attributeName = {attributeValue}")
        .on("attributeValue" -> attributeValue)
        .as(SqlParser.long(1).*)
      // TODO: Measure if the performance would improve if we somehow returned Array[Long]
      // instead of boxed Seq[Long]
    }

  def nodeIdsFiltered(graphName: String, sqlClause: String): Seq[Long] =
    withConnection { implicit connection =>
      rejectUnsafeSQL(sqlClause)
      SQL(s"SELECT id FROM ${nodesTable(graphName)} WHERE $sqlClause")
        .as(SqlParser.long(1).*)
      // TODO: Measure if the performance would improve if we somehow returned Array[Long]
      // instead of boxed Seq[Long]
    }

  /** Returns all node ids in the given Seq which have the given attribute value. */
  def filterNodeIdsUsingAttributeValue(graphName: String,
                                       nodeIds: Seq[Long],
                                       attributeName: String,
                                       attributeValue: String): Seq[Long] =
    withConnection { implicit connection =>
      validateAttributeName(graphName, attributeName)
      SQL(s"SELECT id FROM ${nodesTable(graphName)} WHERE $attributeName = {attributeValue} AND " +
          "id IN " + nodeIds.mkString("(", ", ", ")"))
        .on("attributeValue" -> attributeValue)
        .as(SqlParser.long(1).*)
    }

  /** Returns all node ids in the given Seq which have the given attribute value. */
  def filterNodeIds(graphName: String, nodeIds: Seq[Long], sqlClause: String): Seq[Long] =
    withConnection { implicit connection =>
      rejectUnsafeSQL(sqlClause)
      SQL(s"SELECT id FROM ${nodesTable(graphName)} WHERE $sqlClause AND " +
        "id IN " + nodeIds.mkString("(", ", ", ")"))
        .as(SqlParser.long(1).*)
    }

  // TODO: Read schema from config file (?)
  def getAttributeColumnsFromDB(graphName: String): collection.Set[String] = {
      // TODO: If we keep this after the first release, convert it to use Anorm for consistency.
      val connection = connectionSource.getConnection
      val s = connection.prepareStatement("select column_name from information_schema.columns where " +
        "table_name='${nodesTable(graphName)}'")
      val nameResults = s.executeQuery()
      val result = new mutable.HashSet[String]()
      while (nameResults.next())
        result += nameResults.getString(1)
      result -= "id"
      connection.close()
      result
    }

  def addEdges(graphName: String, ids1: Seq[Long], ids2: Seq[Long]): Unit = {
    // TODO: Support multiple graphs
    val edgesString = (ids1 zip ids2).mkString(", ")
    withConnection { implicit connection =>
      SQL(s"INSERT INTO edges (id1, id2) VALUES $edgesString").executeUpdate()
    }
  }

  def rejectUnsafeSQL(sqlClause: String): Unit = {
    // TODO: Improve SQL injection defense
    if (sqlClause.contains(";"))
      throw new InvalidArgumentException(s"Malformed sql clause: '$sqlClause'")
  }

  def validateAttributeName(graphName: String, attributeName: String): Unit = {
    // TODO: Add support for multiple graphs, then restore this
    //if (! attributeColumns.contains(attributeName)) {
    //  throw new UndefinedAttributeException (s"Undefined attribute $attributeName")
    //}
  }
}
