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

import java.io.File

import co.teapot.mmalloc.MemoryMappedAllocator

/**
  * A graph which stores edge data in a memory mapped file.  There is no object overhead per
  * node: the memory used for with both in-neighbor and out-neighbor access is 8 bytes per edge
  * and 24 bytes per node. Loading is very fast because no parsing of text is required.
  * Loading time is exactly the time it takes the operating system to page data from disk into
  * memory.  Nodes are numbered sequentially from 0 to maxNodeId, and when a node id greater than
  * maxNodeId is added, empty nodes are created as needed (i.e. nodeCount == maxNodeId + 1).
  *
  * If given an empty file, this class creates a new empty graph which will store new nodes and edges
  * in that file.  If a MemMappedDynamicDirectedGraph is later created from the same file, it will
  * contain the added nodes and edges.
  * Graphs can also converted to dynamic binary format using MemMappedDynamicDirectedGraphConverter, then loaded
  * using this class.
  *
  * Concurrent reading threads are safe, but concurrent reading and writing is not currently supported.
  *
  * If parameter syncAllWrites is true, all writes are immediately written to disk during each addEdges
  * call.  If syncAllWrites is set to false, write performance
  * will improve, but the graph may be in an inconsistent state if the OS crashes or the machine loses power.
  *
  * The binary format is currently subject to change.
  */

class MemMappedDynamicDirectedGraph(file: File, syncAllWrites: Boolean = false) extends DynamicDirectedGraph {
  private val initializeNewGraph = !file.exists() || file.length() == 0
  private val mmAllocator = new MemoryMappedAllocator(file)
  mmAllocator.syncAllWrites = syncAllWrites

  private val edgeCountPointer = MemoryMappedAllocator.GlobalApplicationDataPointer
  private val outGraphDataPointer = MemoryMappedAllocator.GlobalApplicationDataPointer + 8L
  private val inGraphDataPointer = MemoryMappedAllocator.GlobalApplicationDataPointer + 16L

  private val outGraph = new MemMappedDynamicUnidirectionalGraph(mmAllocator, outGraphDataPointer, initializeNewGraph)
  private val inGraph = new MemMappedDynamicUnidirectionalGraph(mmAllocator, inGraphDataPointer, initializeNewGraph)

  if (initializeNewGraph) {
    setEdgeCount(0L)
  }

  override def outDegree(id: Int): Int =
    if (existsNode(id))
      outGraph.degree(id)
  else
      defaultNeighbors(id).size

  override def inDegree(id: Int): Int =
    if (existsNode(id))
      inGraph.degree(id)
    else
      defaultNeighbors(id).size

  override def outNeighbors(id: Int): IndexedSeq[Int] =
    if (existsNode(id))
      outGraph.neighbors(id)
    else
      defaultNeighbors(id)

  override def inNeighbors(id: Int): IndexedSeq[Int] =
    if (existsNode(id))
      inGraph.neighbors(id)
    else
      defaultNeighbors(id)

  override def edgeCount: Long = mmAllocator.data.getLong(edgeCountPointer)
  private def setEdgeCount(c: Long): Unit = mmAllocator.data.putLong(edgeCountPointer, c, syncAllWrites)

  override def maxNodeId: Int =  math.max(outGraph.maxNodeId, inGraph.maxNodeId)

  override def nodeIds: Iterable[Int] = 0 to maxNodeId

  override def existsNode(id: Int): Boolean =
    Integer.toUnsignedLong(id) <= Integer.toUnsignedLong(maxNodeId)

  // TODO: We could explicitly maintain the node count, instead of pretending all nodes exist.
  override def nodeCountOption: Option[Int] = Some(maxNodeId + 1)

  /** Adds the given edge to the graph, increasing the number of nodes if needed.
    */
  def addEdge(id1: Int, id2: Int): Unit = {
    setEdgeCount(edgeCount + 1)
    outGraph.addEdge(id1, id2)
    inGraph.addEdge(id2, id1)
  }

  /** Increases maxNodeId and creates neighborless nodes as needed to ensure the given id is a valid id. */
  def ensureValidId(id: Long): Unit = {
    outGraph.ensureValidId(id)
    inGraph.ensureValidId(id)
  }

  def setOutDegreeCapacity(id: Int, outDegree: Int): Unit =
    outGraph.setCapacity(id, outDegree)

  def setInDegreeCapacity(id: Int, inDegree: Int): Unit =
    inGraph.setCapacity(id, inDegree)

  /** Syncs all data to disk and blocks until that completes.
    */
  def syncToDisk(): Unit = {
    mmAllocator.data.syncToDisk()
  }

  /** Loads the underlying memory mapped file to RAM (if it isn't already in RAM), to improve the speed
    * of subsequent reads.  */
  def loadToRam(): Unit = {
    mmAllocator.data.loadFileToRam()
  }
}

object MemMappedDynamicDirectedGraph {
  def apply(graphFile: File): MemMappedDynamicDirectedGraph =
    new MemMappedDynamicDirectedGraph(graphFile, syncAllWrites = false)
  def apply(graphFilename: String): MemMappedDynamicDirectedGraph =
    this(new File(graphFilename))
}
