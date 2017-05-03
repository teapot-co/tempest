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

/** A Large Block is a contiguous sequence of blocks.  The first block has a small header, and
  * the remaining blocks in the Large Block have no headers.*/
private[mmalloc] object LargeBlock {
  // Header Layout
  val BlockTypeOffset = Offset(0)
  val BlockCountOffset = Offset(8)
  // Skip 4 bytes for alignment
  val HeaderSize = ByteCount(16)

  /** A random number, to sanity check block locations. */
  val LargeBlockMagicNumber = 1648927401674812135L
  val FreeBlockMagicNumber = -3040736377681742059L

  def isLargeBlock(pointer: Pointer, data: LargeMappedByteBuffer): Boolean =
    data.getLong(pointer + BlockTypeOffset) == LargeBlockMagicNumber

  def isFreeBlock(pointer: Pointer, data: LargeMappedByteBuffer): Boolean =
    data.getLong(pointer + BlockTypeOffset) == FreeBlockMagicNumber

  /** The number of blocks in this LargeBlock, including this one. */
  def blockCount(pointer: Pointer, data: LargeMappedByteBuffer): Int =
    data.getInt(pointer + BlockCountOffset)

  def initializeLargeBlock(pointer: Pointer, data: LargeMappedByteBuffer, blockCount: Int): Unit = {
    data.putLong(pointer + BlockTypeOffset, LargeBlockMagicNumber)
    data.putInt(pointer + BlockCountOffset, blockCount)
  }
}
