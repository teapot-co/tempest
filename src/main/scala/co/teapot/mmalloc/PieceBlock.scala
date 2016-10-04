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


/** A PieceBlock is a block with a header, a bit-set indicating which pieces are free, and a sequence
  * of uniformly-sized pieces. */
private[mmalloc] class PieceBlock(val pointer: Long, mmAllocator: MemoryMappedAllocator) {
  private def data = mmAllocator.data
  private def syncAllWrites = mmAllocator.syncAllWrites

  require(PieceBlock.isPieceBlock(pointer, mmAllocator.data), s"pointer $pointer points to a block " +
    s"with magic number ${data.getLong(PieceBlock.BlockTypeOffset)}")

  /** The number of bytes per piece. */
  def pieceSize: Int = PieceBlock.pieceSize(pointer, data)

  /** The number of free pieces in this block. */
  def freeCount: Int = PieceBlock.freeCount(pointer, data)

  /** An a speed optimization, we store the location where we last found a set bit in the freeSet.
    * For example, if the first 100,000 bits are 0 and we find a set bit at index 100,001 then we
    * would store a searchStart of 100,001. In range [0, pieceCount). */
  private def searchStart: Int =
    PieceBlock.searchStart(pointer, data)

  /** The total number of pieces stored in this block. */
  val pieceCount = data.getInt(pointer + PieceBlock.PieceCountOffset)

  val freeSet = new ByteBufferBasedBitSet(
    pointer + PieceBlock.BitsetOffset,
    data,
    pieceCount,
    syncAllWrites)

  private def firstPiecePointer: Long = pointer + PieceBlock.BitsetOffset + freeSet.sizeInBytes

  /** Return a pointer to a location in the memory mapped file of size pieceSize bytes. */
  def alloc(): Long = {
    require(PieceBlock.freeCount(pointer, data) > 0, "alloc() called on full PieceBlock")
    val i = freeSet.nextSetBit(searchStart).getOrElse(
      throw new RuntimeException(s"alloc called on PieceBlock with freeCount $freeCount and empty free set"))
    // Update searchStart to shorten the next search.
    PieceBlock.setSearchStart(pointer, data, i)
    freeSet.set(i, false)
    PieceBlock.setFreeCount(pointer, data, PieceBlock.freeCount(pointer, data) - 1, syncAllWrites)
    PieceBlock.log.trace(s"Allocated piece $i from PieceBlock($pointer); new freeCount $freeCount")
    logFreeSet()
    firstPiecePointer + i * pieceSize
  }

  /** Marks the piece starting at the given pointer as free. */
  def free(piecePointer: Long): Unit = {
    require((piecePointer - firstPiecePointer) % pieceSize == 0,
      "Pointer doesn't point to the start of a piece.")
    val i = ((piecePointer - firstPiecePointer) / pieceSize).toInt
    require(freeSet.get(i) == false, s"Attempt to free piece $i which is already free.  PieceBlock=$pointer  PiecePointer=$piecePointer")
    freeSet.set(i, true)
    PieceBlock.setFreeCount(pointer, data, freeCount + 1, syncAllWrites)
    PieceBlock.log.trace(s"Freed piece $i from PieceBlock($pointer); new freeCount $freeCount")
    logFreeSet()
  }

  def logFreeSet(): Unit = {
    PieceBlock.log.trace(s"Top 8*32 bits of freeset for PieceBlock($pointer): " + freeSet.prefixToString(8))
  }
}

private[mmalloc] object PieceBlock {
  val log = Logger.get
  // Header layout
  val BlockTypeOffset = 0
  val PieceSizeOffset = 8
  val PieceCountOffset = 12
  val FreeCountOffset = 16
  val SearchStartOffset = 20
  val BitsetOffset = 24

  /** A random number, to sanity check block locations. */
  val PieceBlockMagicNumber = -6662219058551629080L

  def isPieceBlock(pointer: Long, data: LargeMappedByteBuffer): Boolean =
    data.getLong(pointer + BlockTypeOffset) == PieceBlockMagicNumber

  def pieceSize(pointer: Long, data: LargeMappedByteBuffer): Int =
    data.getInt(pointer + PieceSizeOffset)
  def setPieceSize(pointer: Long, data: LargeMappedByteBuffer, pieceSize: Int, syncWrite: Boolean): Unit = {
    data.putInt(pointer + PieceSizeOffset, pieceSize, syncWrite)
  }

  def pieceCount(pointer: Long, data: LargeMappedByteBuffer): Int =
    data.getInt(pointer + PieceCountOffset)

  def freeCount(pointer: Long, data: LargeMappedByteBuffer): Int =
    data.getInt(pointer + FreeCountOffset)
  def setFreeCount(pointer: Long, data: LargeMappedByteBuffer, freeCount: Int, syncWrite: Boolean): Unit = {
    data.putInt(pointer + FreeCountOffset, freeCount, syncWrite)
  }

  def isFull(pointer: Long, data: LargeMappedByteBuffer): Boolean =
    freeCount(pointer, data) == 0

  def searchStart(pointer: Long, data: LargeMappedByteBuffer): Int =
    data.getInt(pointer + PieceBlock.SearchStartOffset)
  def setSearchStart(pointer: Long, data: LargeMappedByteBuffer, searchStart: Int): Unit = {
    // Don't need to sync, since searchStart is just an optimization hint
    data.putInt(pointer + PieceBlock.SearchStartOffset, searchStart)
  }

  def initializePieceBlock(
      pointer: Long,
      data: LargeMappedByteBuffer,
      pieceSize: Int,
      blockSize: Int,
      syncWrite: Boolean): Unit = {
    setPieceSize(pointer, data, pieceSize, false)
    // For each piece, we store an extra bit (1.0 / 8 bytes) to track if its free.  Hence
    // the number of pieces we can fit is the blockSize (excluding header and 4 bytes for rounding)
    // divided by the pieceSize + 1.0/8
    val pieceCount = ((blockSize - PieceBlock.BitsetOffset - 4) / (pieceSize + 1.0 / 8)).toInt
    data.putLong(pointer + BlockTypeOffset, PieceBlockMagicNumber)
    data.putInt(pointer + PieceSizeOffset, pieceSize)
    data.putInt(pointer + PieceCountOffset, pieceCount)
    data.putInt(pointer + FreeCountOffset, pieceCount)
    data.putInt(pointer + SearchStartOffset, 0)
    ByteBufferBasedBitSet.initializeWith1s(pointer + BitsetOffset, data, pieceCount)
    if (syncWrite) {
      data.syncToDisk(pointer, BitsetOffset + pieceCount / 8)
    }
  }
}
