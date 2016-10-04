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

package co.teapot.mmalloc

import com.twitter.logging.Logger
import it.unimi.dsi.fastutil.longs.LongArrayList


/* Manages a set of blocks and for the given pieceSize.  A block is full if it has no more space for
 * new pieces.  Only non-full blocks are stored in a list,
 * since full blocks are only needed when a pointer is freed, and then their location is
 * computed from the pointer being freed.  The current piece block is not stored in nonFullBlocks. */
private[mmalloc] class PieceAllocator(pieceSize: Int, mmAllocator: MemoryMappedAllocator) {
  val log = Logger.get
  private val nonFullBlocks = new LongArrayList()
  private var currentPieceBlockOption: Option[PieceBlock] = None

  private[mmalloc] def appendNonFullBlock(pointer: Long): Unit =
    nonFullBlocks.push(pointer)

  /** Returns a pointer to a piece of memory of pieceSize bytes.  */
  def alloc(): Long = {
    if (currentPieceBlockOption.isEmpty || currentPieceBlockOption.get.freeCount == 0) {
      if (nonFullBlocks.size == 0) {
        createNewBlock()
      }
      val block = nonFullBlocks.popLong()
      currentPieceBlockOption = Some(new PieceBlock(block, mmAllocator))
    }
    currentPieceBlockOption.get.alloc()
  }

  private def createNewBlock(): Unit = {
    val newBlock = mmAllocator.allocBlock()
    log.trace(s"Creating new block at $newBlock for pieces of size $pieceSize")
    PieceBlock.initializePieceBlock(
      newBlock,
      mmAllocator.data,
      pieceSize,
      mmAllocator.blockSize,
      mmAllocator.syncAllWrites)
    nonFullBlocks.push(newBlock)
  }

  /** Frees the given pointer, which must be part of the given block. */
  def free(pointer: Long, block: PieceBlock): Unit = {
    if (block.freeCount == 0 &&
        (currentPieceBlockOption.isEmpty || block.pointer != currentPieceBlockOption.get.pointer)) {
      nonFullBlocks.push(block.pointer)
    }
    block.free(pointer)
  }
}
