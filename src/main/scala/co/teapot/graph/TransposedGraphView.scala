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

package co.teapot.graph

/**
  * A transposed view of a given graph.
  */
private[graph] class TransposedGraphView(graph: DirectedGraph) extends DirectedGraph {
  override def outNeighbors(id: Int): IndexedSeq[Int] =
    graph.inNeighbors(id)
  override def inNeighbors(id: Int): IndexedSeq[Int] =
    graph.outNeighbors(id)

  override def outNeighbor(id: Int, i: Int): Int =
    graph.inNeighbor(id, i)
  override def inNeighbor(id: Int, i: Int): Int =
    graph.outNeighbor(id, i)

  override def outDegree(id: Int): Int = graph.inDegree(id)
  override def inDegree(id: Int): Int = graph.outDegree(id)

  // These just defer to the underlying graph.
  override def maxNodeId: Int = graph.maxNodeId
  override def existsNode(id: Int): Boolean = graph.existsNode(id)
  override def nodeIds: Iterable[Int] = graph.nodeIds
  override def nodeCountOption: Option[Int] = graph.nodeCountOption
  override def edgeCount: Long = graph.edgeCount // Not precise, but good enough
}
