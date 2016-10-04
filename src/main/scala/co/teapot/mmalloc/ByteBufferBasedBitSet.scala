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

import co.teapot.tempest.util.Util

/** Represents a subset of [0, bitCount) using 4 * ceiling(bitCount / 32) bytes, starting at the
  * given pointer.
  */
/* Bits are accessed in "groups" of 32.  We define the ith bit of a group to be the bit at position
  (1 << i).
 */
private[mmalloc] class ByteBufferBasedBitSet(
    pointer: Long,
    data: LargeMappedByteBuffer,
    bitCount: Int,
    syncAllWrites: Boolean) {
  private val groupCount =  Util.divideRoundingUp(bitCount, 32)
  def sizeInBytes = groupCount * 4

  private def groupIndex(bitIndex: Int): Int = bitIndex / 32
  private def indexWithinGroup(bitIndex: Int): Int = bitIndex % 32

  /** Returns some index in the range [0, bitCount) of a set bit.  To improve running time, the search
    * starts at the given searchStart bit index.  The search wraps around if it reaches the end of
    * the bit-set.  Returns None if no bit is set. */
  def nextSetBit(searchStart: Int = 0): Option[Int] =
    nextBit(searchStart: Int, set=true)

  /** Returns some index in the range [0, bitCount) of a clear (0) bit.  To improve running time, the search
    * starts at the given searchStart bit index.  The search wraps around if it reaches the end of
    * the bit-set.  Returns None if no bit is clear. */
  def nextClearBit(searchStart: Int = 0): Option[Int] =
    nextBit(searchStart: Int, set=false)

  /** Returns some index in the range [0, bitCount) of a bit in the given state (set=true or set=false).
    *  To improve running time, the search starts at the given searchStart bit index.  The search
    *  wraps around if it reaches the end of the bit-set.   Returns None if no bit matches set. */
  def nextBit(searchStart: Int, set: Boolean): Option[Int] = {
    // First find the index of a group with a set bit
    val startGroupI = groupIndex(searchStart)
    for (groupI <- (startGroupI until groupCount).view ++ (0 until startGroupI)) {
      if (doesGroupContainBit(groupI, set)) {
        val group = data.getInt(pointer + groupI * 4)
        // Add in the offset of the set bit within group
        return Some(groupI * 32 + Integer.numberOfTrailingZeros(if (set) group else ~group))
      }
    }
    None
  }

  /** Checks if the ith group contains a bit in the given state. */
  private def doesGroupContainBit(groupI: Int, set: Boolean): Boolean = {
    require(groupI < groupCount)
    val group = data.getInt(pointer + groupI * 4)
    if ((groupI + 1) * 32 <= bitCount) {
      group != (if (set) 0 else ~0)
    } else {
      val bitsInLastGroup = bitCount - groupI * 32
      ((if (set) group else ~group) & ((1 << bitsInLastGroup) - 1)) != 0
    }
  }

  /** Mark the ith bit as set or not set. */
  def set(i: Int, isSet: Boolean): Unit = {
    val p = pointer + 4 * groupIndex(i)
    val newInt = if (isSet) {
      data.getInt(p) | (1 << indexWithinGroup(i))
    } else {
      data.getInt(p) & ~(1 << indexWithinGroup(i))
    }
    data.putInt(p, newInt, syncAllWrites)
  }

  /** Returns the set status of the ith bit. */
  def get(i: Int): Boolean = {
    val p = pointer + 4 * groupIndex(i)
    (data.getInt(p) & (1 << indexWithinGroup(i))) != 0
  }

  /** Returns hexidecimal of the first k 32-bit groups. */
  def prefixToString(k: Int): String = {
    ((0 until k) map { i => data.getInt(pointer + 4 * i).toHexString }).mkString(" ")
  }
}

private[mmalloc] object ByteBufferBasedBitSet {
  def initializeWith1s(pointer: Long, data: LargeMappedByteBuffer, bitCount: Int): Unit = {
    for (i <- 0 until bitCount / 32) {
      data.putInt(pointer + 4 * i, 0xFFFFFFFF)
    }
    data.putInt(pointer + 4 * (bitCount / 32), (1 << (bitCount % 32)) - 1)
  }

  def initializeWith0s(pointer: Long, data: LargeMappedByteBuffer, bitCount: Int): Unit = {
    for (i <- 0 until Util.divideRoundingUp(bitCount, 32)) {
      data.putInt(pointer + 4 * i, 0)
    }
  }
}
