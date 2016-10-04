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

package co.teapot.tempest.graph

/**
  * Given a graph, returns an object which simply forwards all method calls to the given graph.
  * This allows classes like TransposedGraphView to only override the methods they need to override.
  */
class GraphView(graph: DirectedGraph) extends DirectedGraph {
  // Note: it is somewhat clunky to define all the methods here.
  // In theory we could use a compiler plugin as described at
  // http://www.artima.com/weblogs/viewpost.jsp?thread=275135
  // to generate them, but that seems overly complex for now.
  def outDegree(id: Int): Int = graph.outDegree(id)
  def outNeighbors(id: Int): IndexedSeq[Int] = graph.outNeighbors(id)
  def inDegree(id: Int): Int = graph.inDegree(id)
  def inNeighbors(id: Int): IndexedSeq[Int] = graph.inNeighbors(id)
  def existsNode(id: Int): Boolean = graph.existsNode(id)
  def nodeIds: Iterable[Int] = graph.nodeIds
  def maxNodeId: Int = graph.maxNodeId
  def nodeCountOption: Option[Int] = graph.nodeCountOption
  def edgeCount: Long = graph.edgeCount
}
