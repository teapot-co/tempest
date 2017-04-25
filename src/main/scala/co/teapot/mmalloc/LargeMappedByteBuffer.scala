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
import java.nio.ByteOrder
import java.nio.channels.FileChannel

import co.teapot.tempest.util.Util
import com.indeed.util.mmap.{DirectMemory, MMapBuffer}

/** Memory maps a file and provides access to Ints and Longs. Any write operation automatically
  * extends the length of the underlying file.
  */
trait LargeMappedByteBuffer {
  def getInt(index: Pointer): Int
  def putInt(index: Pointer, v: Int, forceToDisk: Boolean = false): Unit

  def getLong(index: Pointer): Long
  def putLong(index: Pointer, v: Long, forceToDisk: Boolean = false): Unit

  def intSeq(start: Pointer, len: Int): IndexedSeq[Int] = new IndexedSeq[Int]() {
    def apply(i: Int): Int = getInt(start + Offset.ints(i))
    def length: Int = len
  }

  def longSeq(start: Pointer, len: Int): IndexedSeq[Long] = new IndexedSeq[Long]() {
    def apply(i: Int): Long = getLong(start + Offset.longs(i))
    def length: Int = len
  }

  /** Copies byteCount bytes from srcIndex to destIndex in this buffer.  */
  def copy(destIndex: Pointer, srcIndex: Pointer, byteCount: ByteCount): Unit

  /** Loads the underlying memory mapped file into physical RAM.
    */
  def loadFileToRam(): Unit

  /** Syncs count bytes at the given index to disk and blocks until that completes.
    */
  def syncToDisk(startIndex: Pointer, count: ByteCount): Unit

  /** Syncs all buffers to disk and blocks until that completes (see MappedByteBuffer.force()).
    */
  def syncToDisk(): Unit

  def close(): Unit
}

class MMapByteBuffer(f: File) extends LargeMappedByteBuffer {
  private var buffer = new MMapBuffer(f, FileChannel.MapMode.READ_WRITE, ByteOrder.nativeOrder())
  private var memory: DirectMemory = buffer.memory()

  /** To prevent repeated small size increases, round all size increases to the
    * next multiple of ChunkSize.
    */
  val ChunkSize = 1024L * 1024L
  val MinExpansionFactor = 1.1 // To prevent many tiny resizes, always resize by at least this factor.

  /** Makes sure that the given index is a valid offset by remapping if needed. */
  def ensureSize(i: Pointer): Unit = {
    if (i.raw >= memory.length) {
      val newSize = Util.divideRoundingUp(math.max(i.raw + 1, (memory.length * MinExpansionFactor).toLong), ChunkSize) * ChunkSize
      println(s"Resizing file from ${memory.length} to $newSize bytes") // TODO: Remove println
      buffer.close()
      buffer = new MMapBuffer(f, 0, newSize, FileChannel.MapMode.READ_WRITE, ByteOrder.nativeOrder())
      memory = buffer.memory()
    }
  }

  override def getInt(index: Pointer): Int = memory.getInt(index.raw)

  def putInt(index: Pointer, v: Int, forceToDisk: Boolean = false): Unit = {
    ensureSize(index + Offset(3))
    memory.putInt(index.raw, v)
    if (forceToDisk)
      buffer.sync(index.raw, 4)
  }

  override def getLong(index: Pointer): Long = memory.getLong(index.raw)
  def getPointer(index: Pointer): Pointer = Pointer(getLong(index))

  def putLong(index: Pointer, v: Long, forceToDisk: Boolean = false): Unit = {
    ensureSize(index + Offset(7))
    memory.putLong(index.raw, v)
    if (forceToDisk)
      buffer.sync(index.raw, 8)
  }
  def putPointer(index: Pointer, v: Pointer, forceToDisk: Boolean = false): Unit =
    putLong(index, v.raw, forceToDisk)

  /** Copies byteCount bytes from srcIndex to destIndex in this buffer.  */
  def copy(destIndex: Pointer, srcIndex: Pointer, byteCount: ByteCount): Unit = {
    ensureSize(destIndex + byteCount.toOffset)
    memory.putBytes(destIndex.raw, memory, srcIndex.raw, byteCount.raw)
  }

  /** Loads the underlying memory mapped file into physical RAM.  Uses mlock.
    */
  def loadFileToRam(): Unit =
    buffer.mlock(0, memory.length)

  /** Syncs the buffer containing the given indexes in the given range to disk and blocks until that completes (see
    * MappedByteBuffer.force()).
    */
  override def syncToDisk(startIndex: Pointer, count: ByteCount): Unit = {
    buffer.sync(startIndex.raw, count.raw)
  }

  /** Syncs all buffers to disk and blocks until that completes (see MappedByteBuffer.force()).
    */
  def syncToDisk(): Unit = syncToDisk(Pointer(0), ByteCount(memory.length()))

  def close(): Unit = buffer.close()
}
