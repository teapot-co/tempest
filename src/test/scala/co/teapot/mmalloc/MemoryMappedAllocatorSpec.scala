package co.teapot.mmalloc

import java.io.File

import co.teapot.util.LogUtil
import org.scalatest.{FlatSpec, Matchers}

class MemoryMappedAllocatorSpec extends FlatSpec with Matchers {
  LogUtil.configureConsoleLog("")
  "A MemoryMappedAllocator" should "allocate pieces correctly" in {
    val f = File.createTempFile("foo", ".dat")
    f.deleteOnExit()
    val pieceSizes = Array(100, 200)
    val blockSize = 232
    val a = new MemoryMappedAllocator(f, pieceSizes, blockSize)
    // With header, a piece size block should have space for two pieces
    val p1 = a.alloc(100)
    val p2 = a.alloc(100)
    a.allocatedBlockCount should equal (1)

    // Make sure allocator re-uses pieces after they're freed
    a.free(p1)
    val p3 = a.alloc(100)
    p3 should equal (p1)
    a.free(p2)
    val p4 = a.alloc(100)
    p4 should equal (p2)
    a.allocatedBlockCount should equal (1)

    // Check that allocator creates a new piece block when needed
    a.alloc(100)
    a.allocatedBlockCount should equal (2)
    a.alloc(101) // Should cause a pieceBlock for size 200 pieces to be created
    a.allocatedBlockCount should equal (3)

    // Make sure that allocator goes back to the first block when the 2nd block fills
    a.free(p2)
    a.alloc(100)
    val p8 = a.alloc(100)
    p8 should equal (p2)

    a.free(p8)
    // Make sure the file can be reopened by a new allocator instance
    val a2 = new MemoryMappedAllocator(f, pieceSizes, blockSize)
    val p9 = a2.alloc(100)
    p9 should equal (p2)
  }

  "A MemoryMappedAllocator" should "allocate large blocks correctly" in {
    val f = File.createTempFile("foo", ".dat")
    f.deleteOnExit()
    val a = new MemoryMappedAllocator(f, pieceSizes = Array(100, 200), blockSize = 232)
    val p1 = a.alloc(400) // 2 blocks
    a.allocatedBlockCount should equal(2)
    // Write arbitrary data at p1 to make sure p1 is past the header
    a.data.putLong(p1, 0x1234567812345678L)
    a.data.putLong(p1 + 8, 0x1234567812345678L)

    val p2 = a.alloc(600) // 3 blocks
    a.allocatedBlockCount should equal(5)
    assert((p2 - p1) >= 400)

    // Make sure the freed blocks can be used for pieces.
    a.free(p1) // frees two blocks
    a.alloc(100)
    a.alloc(200)
    a.allocatedBlockCount should equal (5)
  }
}
