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

import java.io.File
import java.util
import co.teapot.util.Util
import com.twitter.logging.Logger
import it.unimi.dsi.fastutil.longs.LongArrayList

import MemoryMappedAllocator._ // Make constants available

/** Manages memory and provides functions similar to C's standard library, including functions to
  * allocate memory, extend an allocation, and free memory. All "pointers" are byte offsets into data,
  * a LargeMappedByteBuffer.  */
trait Allocator {
  /** Returns a pointer to a piece within data of size at least byteCount bytes.*/
  def alloc(byteCount: Long): Long

  /** Returns a pointer to a piece within data of size at least byteCount bytes.  Copies all
    * data from the allocation at the given originalPointer to the new pointer.  If freeOriginal
    * is true, the allocation at the original pointer is freed.*/
  def realloc(originalPointer: Long, byteCount: Long/*, freeOriginal: Boolean = true*/): Long

  /** Free the memory pointed  */
  def free(pointer: Long)

  /** All pointers refer to locations within data. */
  def data: LargeMappedByteBuffer
}

/** Allocates memory backed by an underlying memory-mapped file.  If the given file is empty,
  * initializes it.  The file grows automatically as allocations are performed.
  *
  * For tuning performance, the parameter blockSize controls the unit of memory internally allocated.
  * The parameter pieceSizes controls the granularity of allocations. When allocating k bytes,
  * the allocator pads k up to the next element in pieceSizes, or to the next multiple of blockSize
  * if k is larger than the largest element of pieceSizes.  The array pieceSizes must be sorted,
  * and the largest piece size must be smaller than blockSize by at least ~32 bytes (the minimum
  * size of a PieceBlock header). If blockSize or pieceSizes are given when a file is first created,
  * the same parameters must be used when MemoryMappedAllocator is constructed with the same file.
  *
  * This class is thread-safe (multiple threads may call the methods concurrently).  (TODO: double-check thread-safety)
  */

/* Allocations are either large or small.  Large allocations use a contiguous sequence of blocks.
   Small allocations
   (those whose size is at most the largest pieceSize) use one piece of divided block.  For each
   pieceSize, a PieceSizeAllocator manages the blocks assigned to that piece size.  The ith block
   is the blockSize bytes starting at (HeaderSize + i * blockSize to HeaderSize).
   For each block, either
     a) the block is managed by a PieceAllocator
     b) the block is part of a large allocation
     c) the block is in the freeBlocks list.
   */

class MemoryMappedAllocator(dataFile: File,
                            val pieceSizes: Array[Int] = MemoryMappedAllocator.DefaultPieceSizes,
                            val blockSize: Int = MemoryMappedAllocator.DefaultBlockSize
                           ) extends Allocator {
  val log = Logger.get
  log.info(s"allocator data file ${dataFile.getAbsolutePath} has length " + dataFile.length())
  val initializeNewAllocator: Boolean = !dataFile.exists() || dataFile.length() == 0

  override val data = new MMapByteBuffer(dataFile)

  private[mmalloc] def allocatedBlockCount = data.getInt(AllocatedBlockCountPointer)

  private val largestPieceSize = pieceSizes.last
  require(pieceSizes.sorted.sameElements(pieceSizes))
  require(largestPieceSize + PieceBlock.BitsetOffset + 4 <= blockSize)

  // Blocks are identified by a pointer to their first byte
  private val freeBlocks = new LongArrayList()

  private val pieceAllocators: Array[PieceAllocator] = Array.tabulate(pieceSizes.length) { i =>
    new PieceAllocator(pieceSizes(i), this)
  }

  if (initializeNewAllocator)
    initializeNewFile()
  else
    recoverFreeBlockLists()

  /** Reads the header of all blocks to determine their type, and adds blocks to the
    * free block list or appropriate pieceAllocator.
    */
  private def recoverFreeBlockLists(): Unit = {
    // Use var i so we can jump past blocks in large allocations.
    var i = 0
    while (i < allocatedBlockCount) {
      val pointer = HeaderSize + i.toLong * blockSize.toLong
      if (PieceBlock.isPieceBlock(pointer, data)) {
        if (! PieceBlock.isFull(pointer, data)) {
          val pieceSize = PieceBlock.pieceSize(pointer, data)
          pieceAllocatorForSize(pieceSize).appendNonFullBlock(pointer)
        }
        i += 1
      } else if (LargeBlock.isLargeBlock(pointer, data)) {
        val blockCount = LargeBlock.blockCount(pointer, data)
        i += blockCount
      } else {
        require(LargeBlock.isFreeBlock(pointer, data),
          s"Block pointer $pointer with magic number ${data.getLong(pointer + LargeBlock.BlockTypeOffset)} "
          + "has invalid block type")
        freeBlocks.push(pointer)
        i += 1
      }
    }
    log.info(s"Read $allocatedBlockCount blocks, and found ${freeBlocks.size()} free blocks")
  }

  /** Reads the header of all blocks to determine their type, and adds appropriate blocks to the
    * free block list or appropriate pieceAllocator.
    */
  private def initializeNewFile(): Unit = {
    log.info(s"Initializing new allocator on $dataFile")
    data.putInt(VersionPointer, CurrentVersion, syncAllWrites)
    data.putInt(AllocatedBlockCountPointer, 0, syncAllWrites)
  }

  /** Returns a pointer to the first byte of a new block. The data in the block is undefined.*/
  private[mmalloc] def allocBlock(): Long = {
    if (freeBlocks.size > 0) {
      freeBlocks.popLong()
    } else {
      val newPointer = allocatedBlockCount * blockSize.toLong + HeaderSize
      data.putInt(AllocatedBlockCountPointer, allocatedBlockCount + 1, syncAllWrites)
      log.trace(s"new block pointer: $newPointer; blockCount $allocatedBlockCount")
      newPointer
    }
  }

  /** Returns the PieceAllocator for the smallest pieceSize >= the given size. */
  private def pieceAllocatorForSize(size: Int): PieceAllocator = {
    assert (size <= largestPieceSize)
    val signedI = util.Arrays.binarySearch(pieceSizes, size.toInt)
    val pieceSizeI = if (signedI >= 0)
      signedI
    else
      -signedI - 1 // The smallest index with value greater then size

    assert(pieceSizes(pieceSizeI) >= size.toInt)
    assert(pieceSizeI == 0 || pieceSizes(pieceSizeI - 1) < size.toInt)
    pieceAllocators(pieceSizeI)
  }

  /** Given a pointer, returns a pointer to the first byte of the block containing the pointer. */
  private[mmalloc] def pointerToBlock(pointer: Long): Long = {
    require(pointer >= HeaderSize, s"pointer $pointer < HeaderSize $HeaderSize")
    val blockI = (pointer - HeaderSize) / blockSize.toLong
    HeaderSize + blockI * blockSize.toLong
  }

  /** Assuming the given pointer was allocated by this allocator, returns the number of bytes in
    * its allocation.
    */
  def allocationCapacity(pointer: Long): Long = {
    val blockPointer = pointerToBlock(pointer)
      if (PieceBlock.isPieceBlock(blockPointer, data)) {
        PieceBlock.pieceSize(blockPointer, data)
      } else {
        require(LargeBlock.isLargeBlock(blockPointer, data),
          s"computing capacity (e.g. through realloc) of pointer $pointer led to blockPointer " +
            s"$blockPointer with magic number " +
            data.getLong(blockPointer + LargeBlock.BlockTypeOffset))
        LargeBlock.blockCount(blockPointer, data) * blockSize.toLong - LargeBlock.HeaderSize
      }
  }

  override def alloc(size: Long): Long =
    this.synchronized {
      if (size > largestPieceSize) {
        val newBlockCount = Util.divideRoundingUp(size + LargeBlock.HeaderSize, blockSize.toLong).toInt
        val largeBlockStart = HeaderSize + allocatedBlockCount * blockSize.toLong
        data.putInt(AllocatedBlockCountPointer, allocatedBlockCount + newBlockCount, forceToDisk=syncAllWrites)
        LargeBlock.initializeLargeBlock(largeBlockStart, data, newBlockCount)
        val dataStart = largeBlockStart + LargeBlock.HeaderSize
        log.trace(s"Allocated $newBlockCount blocks starting at $largeBlockStart for allocation of size $size")
        dataStart
      } else {
        pieceAllocatorForSize(size.toInt).alloc()
      }
    }

  override def realloc(originalPointer: Long, byteCount: Long /*, freeOriginal: Boolean = true*/): Long =
    this.synchronized {
      val oldCapacity = allocationCapacity(originalPointer)

      if (oldCapacity > byteCount) {
        originalPointer // In this case, nothing needs to be done
      } else {
        val newPointer = alloc(byteCount)
        data.copy(newPointer, originalPointer, byteCount)
        free(originalPointer)
        newPointer
      }
    }

  override def free(pointer: Long): Unit =
    this.synchronized {
      val blockPointer = pointerToBlock(pointer)
      if (PieceBlock.isPieceBlock(blockPointer, data)) {
        val block = new PieceBlock(blockPointer, this)
        pieceAllocatorForSize(block.pieceSize).free(pointer, block)
      } else {
        require(LargeBlock.isLargeBlock(blockPointer, data),
          s"freeing pointer $pointer led to blockPointer $blockPointer with magic number " +
            data.getLong(blockPointer + LargeBlock.BlockTypeOffset))
        val blockCount = LargeBlock.blockCount(blockPointer, data)
        for (i <- 0 until blockCount) {
          val freeBlockPointer = blockPointer + i.toLong * blockSize.toLong
          data.putLong(freeBlockPointer + LargeBlock.BlockTypeOffset, LargeBlock.FreeBlockMagicNumber)
          freeBlocks.push(freeBlockPointer)
        }
      }
    }

  /** Indicates that all writes should be synced to the underlying disk immediately.  If set to false,
    * write speed will increase, but the allocator may be in an inconsistent state if the process
    * terminates unexpectedly.
    * */
  var syncAllWrites: Boolean = true
}

object MemoryMappedAllocator {
  // Header data
  val VersionPointer = 0L
  val AllocatedBlockCountPointer = 4L
  /** Users can store global variables between this pointer and HeaderSize. */
  val GlobalApplicationDataPointer = 8L
  // Make headerSize a multiple of a typical page size for better virtual memory alignment.
  val HeaderSize = 4096

  val CurrentVersion = 0

  // By default we use ~1MB per block, so we don't have more than 1 million blocks for a 1TB file,
  // and this keeps the per-block overhead reasonable.
  // We actually use 1MB + 4KB per block, so even after the header is stored, the block can store a
  // 1 MB piece.  We use 4KB because that is a typical page size.
  val DefaultBlockSize = (1 << 20) + (1 << 12)

  // As a reasonable trade-off between realloc cost, overhead per piece size, and the internal
  // fragmentation cost of pieces being larger than requested,
  // by default we use sizes of the form 2**k and 2**k * (3/2).
  // Since our default block size is a bit over 2**20, we exclude 2**20 * 3/4 and 2**20 * 3/8,
  // since we can only store one piece of size 2**20 * 3/4 in a block, and only two pieces of size
  // 2**20 * 3/8 in a block.
  val DefaultPieceSizes: Array[Int] =
    (((1 to 20) map  (i => 1 << i)) ++
      ((1 to 18) map  (i => (1 << i) * 3 / 2))
      ).sorted.toArray
}
