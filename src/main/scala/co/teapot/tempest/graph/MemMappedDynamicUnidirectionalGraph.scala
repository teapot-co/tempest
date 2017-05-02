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
import co.teapot.mmalloc.{ByteCount, MemoryMappedAllocator, Offset, Pointer}
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
                                          dataPointerPointer: Pointer,
                                          initializeNewGraph: Boolean,
                                          msDelayFreeingAllocations: Long = 1000 * 60 /* TODO: Read from config file */) {
  val log = Logger.get
  private def data = allocator.data

  var allocationsToFree = new LongArrayList()
  var previousAllocationsToFree = new LongArrayList()
  setupAllocationFreeingThread()

  if (initializeNewGraph) {
    setDataPointer(allocator.alloc(NodeArrayOffset.toByteCount)) // Space for graph variables and 0 nodes
    setMaxNodeId(-1L)
    setNodeCapacity(0L)
  }

  // At dataPointer, we store
  //   1) an 8-byte maxNodeId
  //   2) an 8-byte nodeCapacity
  //   3) a (12 * nodeCapacity)-byte array of node data
  //        (4-byte degree and an 8-byte neighborPointer per node)

  def dataPointer: Pointer = data.getPointer(dataPointerPointer)
  private def setDataPointer(p: Pointer): Unit = data.putPointer(dataPointerPointer, p)

  // We store maxNodeId as a long to support ids between 2**31 and 2**32.
  def maxNodeId: Int = {
    val result = maxNodeIdLong
    require(result < Int.MaxValue)
    result.toInt
  }
  def maxNodeIdLong: Long =
    data.getLong(dataPointer + MaxNodeIdOffset)
  private def setMaxNodeId(id: Long): Unit = {
    data.putLong(dataPointer + MaxNodeIdOffset, id, allocator.syncAllWrites)
  }

  private def nodeCapacity: Int = {
    val result = data.getLong(dataPointer + NodeCapacityOffset)
    require(result < Int.MaxValue)
    result.toInt
  }
  private def setNodeCapacity(c: Long): Unit = data.putLong(dataPointer + NodeCapacityOffset, c, allocator.syncAllWrites)

  private def nodeArrayPointer: Pointer = dataPointer + NodeArrayOffset

  /** Returns a pointer to a node's (degree, neighborPointer) pair.
    * */
  private def nodePointer(id: Int): Pointer = {
    require(Integer.toUnsignedLong(id) < nodeCapacity, s"id ${Integer.toUnsignedLong(id)} >= nodeCapacity $nodeCapacity")
    nodeArrayPointer + Offset.blocks(id, BytesPerNode)
  }

  def degree(id: Int): Int =
    data.getInt(nodePointer(id))
  private def setDegree(id: Int, d: Int, sync: Boolean = allocator.syncAllWrites): Unit =
    data.putInt(nodePointer(id), d, sync)

  /** Points to the first byte of the neighbors of node id. */
  private def neighborsPointer(id: Int): Pointer = {
    data.getPointer(nodePointer(id) + NodeNeighborOffset)
  }
  private def setNeighborsPointer(id: Int, p: Pointer, sync: Boolean = allocator.syncAllWrites): Unit = {
    // Note: This write might not be aligned to a multiple of 16 bytes. LargeMemoryMappedByteBuffer
    // putLong allows un-aligned longs.
    data.putPointer(nodePointer(id) + NodeNeighborOffset, p, sync)
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
      ensureValidId(math.max(id1, id2))
      val oldDegree = degree(id1)
      ensureSufficientCapacity(id1, oldDegree + 1)
      // For concurrent readers, add edge before incrementing degree
      val neighborsOffset = Offset.blocks(oldDegree, BytesPerNeighbor)
      data.putInt(neighborsPointer(id1) + neighborsOffset, id2, allocator.syncAllWrites)
      setDegree(id1, oldDegree + 1)
    }

  /** Ensures the given id has space for newDegree neighbors by calling setCapacity if needed. */
  private def ensureSufficientCapacity(id: Int, newDegree: Int) = {
    if (neighborsPointer(id) == NullPointer ||
        allocator.allocationCapacity(neighborsPointer(id)) < ByteCount.forNodes(newDegree)) {
      setCapacity(id, Util.nextLeadingTwoBitNumber(newDegree))
    }
  }

  /** Ensures the given id has space for newCapacity neighbors by allocating a new neighbor area.
    * This method doesn't need to be called by clients, but it can be called to improve performance
    * if the client is going to add many new neighbors to the given node id.  */
  def setCapacity(id: Int, newNodeCapacity: Int): Unit = {
    log.trace(s"increasing node $id's neighbor capacity to $newNodeCapacity")
    ensureValidId(id)
    val oldPointer = neighborsPointer(id)
    val newPointer = allocator.alloc(ByteCount.forNodes(newNodeCapacity))
    if (oldPointer != NullPointer) {
      allocator.data.copy(newPointer, oldPointer, ByteCount.forNodes(degree(id)))
      allocationsToFree.push(oldPointer.raw) // For concurrent readers, don't immediately free.
    }
    setNeighborsPointer(id, newPointer)
  }

  /** Increases maxNodeId to be at least id, and extends the node data array if needed to ensure
    * the given id is a valid id. */
  def ensureValidId(id: Int): Unit =
    this.synchronized {
      val oldMaxId = maxNodeId
      if (id > oldMaxId) {
        if (id >= nodeCapacity) {
          val newCapacity = Util.nextLeadingTwoBitNumber(id + 1)
          log.info(s"increasing node capacity to $newCapacity")
          val newByteCountForNodes = Offset.blocks(newCapacity, BytesPerNode).toByteCount
          val newPointer = allocator.alloc(newByteCountForNodes + NodeArrayOffset.toByteCount)
          val oldByteCountForNodes = Offset.blocks(nodeCapacity, BytesPerNode).toByteCount
          allocator.data.copy(newPointer, dataPointer, oldByteCountForNodes + NodeArrayOffset.toByteCount)
          allocationsToFree.push(dataPointer.raw) // For concurrent readers, don't immediately free.
          setDataPointer(newPointer) // For concurrent readers, update dataPointer after copying data.
          setNodeCapacity(newCapacity)
          }
        val nodeArrayCapacity = Offset.blocks(maxNodeId+1, BytesPerNode)
        assert(allocator.allocationCapacity(dataPointer) >= (NodeArrayOffset + nodeArrayCapacity).toByteCount,
          s"data array capacity ${allocator.allocationCapacity(dataPointer)} insufficient for maxNodeId $maxNodeIdLong")
        // Initialize new nodes
        for (i <- Util.longRange(oldMaxId + 1, id + 1)) {
          setDegree(i.toInt, 0, sync=false)
          setNeighborsPointer(i.toInt, NullPointer, sync=false)
        }
        if (allocator.syncAllWrites) {
          data.syncToDisk(nodePointer(0), (nodePointer(id.toInt) - nodePointer(0)).toByteCount + BytesPerNode)
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
      log.info(s"Freeing $previousAllocationsToFree old allocations")
    for (i <- previousAllocationsToFree.toLongArray()) {
      allocator.free(Pointer(i))
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
  val MaxNodeIdOffset = Offset(0L)
  val NodeCapacityOffset = Offset(8L)
  val NodeArrayOffset = Offset(16L)

  val BytesPerDegree = ByteCount(4)
  // For each node, we store a 4-byte degree and an 8-byte neighborPointer.
  val BytesPerNode = BytesPerDegree + Pointer.PointerByteCount
  val BytesPerNeighbor = ByteCount(4)
  val NodeNeighborOffset = Offset(4L)

  val NullPointer = Pointer(-1L) // Used for neighbor pointer of nodes with no neighbors
}
