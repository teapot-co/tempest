package co.teapot.mmalloc

import java.io.File

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class LargeMappedByteBufferSpec extends FlatSpec with Matchers {
  "A LargeMappedByteBuffer " should "create a new file" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    val b = new MMapByteBuffer(f)
    val memLoc = Pointer(1234 + (1L << 30))
    b.putInt(memLoc, 42)
    b.getInt(memLoc) shouldEqual (42)
    b.syncToDisk(memLoc, ByteCount(4))
    val b2 = new MMapByteBuffer(f)
    b2.getInt(Pointer(1234 + (1L<< 30))) shouldEqual (42)
  }

  it should "support copying" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    val b = new MMapByteBuffer(f)
    // Test copying within buffers
    val srcP = Pointer(4)
    b.putInt(srcP, 0x12345678)
    b.copy(Pointer(20), srcP, ByteCount(4))
    b.getInt(Pointer(20)) should equal (0x12345678)
    b.copy(Pointer(256), srcP, ByteCount(4))
    b.getInt(Pointer(256)) should equal (0x12345678)

    // Test multi-buffer copying
    val intCount = 456
    val start = Pointer(16)
    val dest = Pointer(4128)
    val values = Array.fill(intCount)(Random.self.nextInt())

    for ((v, i) <- values.zipWithIndex) {
      b.putInt(start + Offset.ints(i), v)
    }
    b.copy(dest, start, ByteCount(4 * intCount))
    b.intSeq(dest, intCount) should contain theSameElementsInOrderAs (values)

    // Make sure copying doesn't overwrite before or after the target
    b.putInt(dest - Offset(4), 0x76543210)
    b.putInt(dest + Offset.ints(intCount), 0x43211234)
    b.copy(dest, start, ByteCount(4 * intCount))
    b.getInt(dest - Offset(4)) should equal (0x76543210)
    b.getInt(dest + Offset.ints(intCount)) should equal (0x43211234)

    // try copying with start or dest on buffer boundaries
    b.copy(dest, Pointer(100), ByteCount(200))
    b.getInt(dest) should equal (b.getInt(Pointer(100)))
    b.copy(start, Pointer(1000), ByteCount(200))
    b.getInt(Pointer(1000)) should equal (b.getInt(start))
    b.copy(Pointer(1000), Pointer(100), ByteCount(200))
    b.getInt(Pointer(100)) should equal(b.getInt(Pointer(1000)))
    b.getInt(Pointer(296)) should equal(b.getInt(Pointer(1196)))
  }

  it should "support longSeq" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    val b = new MMapByteBuffer(f)

    // Test Long Seq
    val start = Pointer(240L)
    val dest = Pointer(16L)
    val longs = Array.fill(20)(Random.self.nextLong())
    for ((v, i) <- longs.zipWithIndex) {
      b.putLong(start + Offset.longs(i), v)
    }
    b.copy(dest, start, ByteCount(8 * longs.size))
    b.longSeq(dest, longs.size) should contain theSameElementsInOrderAs (longs)
  }
}
