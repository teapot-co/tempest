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

import java.util.{Timer, TimerTask}

import co.teapot.tempest.graph.MemMappedDynamicUnidirectionalGraph._
import co.teapot.mmalloc.MemoryMappedAllocator
import co.teapot.tempest.util.Util
import com.twitter.logging.Logger
import it.unimi.dsi.fastutil.longs.LongArrayList

/** A graph which stores the neighbors of each node in only one direction.  As a result of a call to
  * addEdge(u, v), neighbors(u) will contain v, but neighbors(v) won't contain u.
  *
  * Parameter dataPointerPointer is a pointer to a Long value in allocator.data which this class uses to store the
  * pointer to its data.  If initializeNewGraph is true, it ignores the current value at dataPointerPointer
  * and sets it to a new allocation; else it reads the data at dataPointerPointer to open an existing
  * graph.
  *
  * Currently does not keep track of which nodes exist, but assumes all nodes between 0 and maxNodeId exist.
  *
  * Supports up to 2**32 nodes by using Integer.toUnsignedLong to interpret negative node ids as
  * positive integers.
  *
  */
class MemMappedDynamicUnidirectionalGraph(allocator: MemoryMappedAllocator,
                                          dataPointerPointer: Long,
                                          initializeNewGraph: Boolean,
                                          msDelayFreeingAllocations: Long = 1000 * 60 /* TODO: Read from config file */) {
  val log = Logger.get
  private def data = allocator.data

  var allocationsToFree = new LongArrayList()
  var previousAllocationsToFree = new LongArrayList()
  setupAllocationFreeingThread()

  if (initializeNewGraph) {
    setDataPointer(allocator.alloc(NodeArrayOffset)) // Space for graph variables and 0 nodes
    setMaxNodeId(-1L)
    setNodeCapacity(0L)
  }

  // At dataPointer, we store
  //   1) an 8-byte maxNodeId
  //   2) an 8-byte nodeCapacity
  //   3) a (12 * nodeCapacity)-byte array of node data
  //        (4-byte degree and an 8-byte neighborPointer per node)

  def dataPointer = data.getLong(dataPointerPointer)
  private def setDataPointer(p: Long): Unit = data.putLong(dataPointerPointer, p)

  // We store maxNodeId as a long to support ids between 2**31 and 2**32.
  def maxNodeId: Int = maxNodeIdLong.toInt
  def maxNodeIdLong: Long =
    data.getLong(dataPointer + MaxNodeIdOffset)
  private def setMaxNodeId(id: Long): Unit = {
    data.putLong(dataPointer + MaxNodeIdOffset, id, allocator.syncAllWrites)
  }

  private def nodeCapacity: Long = data.getLong(dataPointer + NodeCapacityOffset)
  private def setNodeCapacity(c: Long): Unit = data.putLong(dataPointer + NodeCapacityOffset, c, allocator.syncAllWrites)

  private def nodeArrayPointer: Long = dataPointer + NodeArrayOffset

  /** Returns a pointer to a node's (degree, neighborPointer) pair.
    * */
  private def nodePointer(id: Int): Long = {
    require(Integer.toUnsignedLong(id) < nodeCapacity, s"id ${Integer.toUnsignedLong(id)} >= nodeCapacity $nodeCapacity")
    nodeArrayPointer + BytesPerNode * Integer.toUnsignedLong(id)
  }

  def degree(id: Int): Int =
    data.getInt(nodePointer(id))
  private def setDegree(id: Int, d: Int, sync: Boolean = allocator.syncAllWrites): Unit =
    data.putInt(nodePointer(id), d, sync)

  /** Points to the first byte of the neighbors of node id. */
  private def neighborsPointer(id: Int): Long = {
    data.getLong(nodePointer(id) + NodeNeighborOffset)
  }
  private def setNeighborsPointer(id: Int, p: Long, sync: Boolean = allocator.syncAllWrites): Unit = {
    // Note: This write might not be aligned to a multiple of 16 bytes. LargeMemoryMappedByteBuffer
    // putLong allows un-aligned longs.
    data.putLong(nodePointer(id) + NodeNeighborOffset, p, sync)
  }

  def neighbors(id: Int): IndexedSeq[Int] = {
    data.intSeq(neighborsPointer(id), degree(id))
  }

  def addEdges(id1s: collection.Seq[Int], id2s: collection.Seq[Int]) = {
    // TODO: Consider optimizing this by adding all edges from a given node together
    for ((id1, id2) <- id1s zip id2s) {
      addEdge(id1, id2)
    }
  }

  /** Appends id2 to the neighbors of id1. */
  def addEdge(id1: Int, id2: Int): Unit =
    this.synchronized {
      ensureValidId(math.max(Integer.toUnsignedLong(id1), Integer.toUnsignedLong(id2)))
      val oldDegree = degree(id1)
      ensureSufficientCapacity(id1, oldDegree + 1)
      // For concurrent readers, add edge before incrementing degree
      data.putInt(neighborsPointer(id1) + 4 * oldDegree, id2, allocator.syncAllWrites)
      setDegree(id1, oldDegree + 1)
    }

  /** Ensures the given id has space for newDegree neighbors by calling setCapacity if needed. */
  private def ensureSufficientCapacity(id: Int, newDegree: Int) = {
    if (neighborsPointer(id) == NullPointer ||
        allocator.allocationCapacity(neighborsPointer(id)) < 4 * newDegree) {
      setCapacity(id, Util.nextLeadingTwoBitNumber(4 * newDegree))
    }
  }

  /** Ensures the given id has space for newCapacity neighbors by allocating a new neighbor area.
    * This method doesn't need to be called by clients, but it can be called to improve performance
    * if the client is going to add many new neighbors to the given node id.  */
  def setCapacity(id: Int, newCapacity: Int): Unit = {
    log.trace(s"increasing node ${id}'s neighbor capacity to $newCapacity")
    ensureValidId(Integer.toUnsignedLong(id))
    val oldPointer = neighborsPointer(id)
    val newPointer = allocator.alloc(4 * newCapacity)
    if (oldPointer != NullPointer) {
      allocator.data.copy(newPointer, oldPointer, 4 * degree(id))
      allocationsToFree.push(oldPointer) // For concurrent readers, don't immediately free.
    }
    setNeighborsPointer(id, newPointer)
  }

  /** Increases maxNodeId to be at least id, and extends the node data array if needed to ensure
    * the given id is a valid id. */
  def ensureValidId(id: Long): Unit =
    this.synchronized {
      val oldMaxId = maxNodeIdLong
      if (id > oldMaxId) {
        if (id >= nodeCapacity) {
          val newCapacity = Util.nextLeadingTwoBitNumber(id + 1L)
          log.info(s"increasing node capacity to $newCapacity")
          val newPointer = allocator.alloc(BytesPerNode * newCapacity + NodeArrayOffset)
          allocator.data.copy(newPointer, dataPointer, BytesPerNode * nodeCapacity + NodeArrayOffset)
          allocationsToFree.push(dataPointer) // For concurrent readers, don't immediately free.
          setDataPointer(newPointer) // For concurrent readers, update dataPointer after copying data.
          setNodeCapacity(newCapacity)
          }
        assert(allocator.allocationCapacity(dataPointer) >= NodeArrayOffset + 12 * (maxNodeIdLong + 1),
          s"data array capacity ${allocator.allocationCapacity(dataPointer)} insufficient for maxNodeId $maxNodeIdLong")
        // Initialize new nodes
        for (i <- Util.longRange(oldMaxId + 1, id + 1)) {
          setDegree(i.toInt, 0, sync=false)
          setNeighborsPointer(i.toInt, NullPointer, sync=false)
        }
        if (allocator.syncAllWrites) {
          data.syncToDisk(nodePointer(0), nodePointer(id.toInt) - nodePointer(0) + BytesPerNode)
        }
        setMaxNodeId(id) // For concurrent readers, update maxNodeId last.
      }
    }

  /* Because there might be concurrent reading threads, we don't actually free the neighbor list of a
   * node until msDelayFreeingAllocations milliseconds after the node's neighbor pointer has been
    * updated to a new neighbor list. */
  def setupAllocationFreeingThread(): Unit = {
    // Setting isDaemon = true makes the thread stop automatically when the application stops
    new Timer(/* isDaemon = */ true).scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = freeOldAllocations()
    }, msDelayFreeingAllocations, msDelayFreeingAllocations)
  }
  private def freeOldAllocations(): Unit = {
    if (previousAllocationsToFree.size > 0)
      log.info(s"Freeing ${previousAllocationsToFree} old allocations")
    for (i <- previousAllocationsToFree.toLongArray()) {
      allocator.free(i)
    }
    previousAllocationsToFree = allocationsToFree
    allocationsToFree = new LongArrayList()
  }
  override def finalize(): Unit = {
    // When the application terminates, hopefully this is called.
    // TODO: Store allocationsToFree in the mmapped file so it can be freed when the application starts.
    log.info("UnidirectionalGraph finalizing by freeing old allocations")
    freeOldAllocations()
    freeOldAllocations()
  }
}

object MemMappedDynamicUnidirectionalGraph {
  // Offsets relative to dataPointer where graph variables are stored:
  val MaxNodeIdOffset = 0L
  val NodeCapacityOffset = 8L
  val NodeArrayOffset = 16L

  // For each node, we store a 4-byte degree and an 8-byte neighborPointer.
  val BytesPerNode = 12
  val NodeNeighborOffset = 4L

  val NullPointer = -1L // Used for neighbor pointer of nodes with no neighbors
}
