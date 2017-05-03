package co.teapot.mmalloc

import java.io.File

import org.scalatest.{Matchers, FlatSpec}

import scala.util.Random

class ByteBufferBasedBitSetSpec extends FlatSpec with Matchers {
  "A ByteBufferBasedBitSet " should "behave correctly" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    val buffer = new MMapByteBuffer(f)
    val pointer = Pointer(42) // Arbitrary
    val bitCount = 201
    ByteBufferBasedBitSet.initializeWith1s(pointer, buffer, bitCount)
    val s = new ByteBufferBasedBitSet(pointer, buffer, bitCount, false)
    for (i <- 0 until bitCount) {
      s.nextSetBit(0) should equal (Some(i))
      s.nextSetBit(i) should equal (Some(i))
      s.get(i) should be (true)
      s.set(i, false)
      s.get(i) should be (false)
    }
    s.nextSetBit(0) should equal (None)

    // Verify search wraps around if given a large searchStart
    s.set(11, true)
    s.nextSetBit(100) should equal (Some(11))
  }

  it should "behave correctly when bitCount varies" in {
    val f = File.createTempFile("test", ".dat")
    f.deleteOnExit()
    val buffer = new MMapByteBuffer(f)
    val bitCounts = Seq(128, 125, 97, 96)
    val pointer0 = Pointer(64) // Arbitrary
    val pointer1 = Pointer(80) // Arbitrary
    for (bitCount <- bitCounts) {
      ByteBufferBasedBitSet.initializeWith0s(pointer0, buffer, bitCount)
      val s0 = new ByteBufferBasedBitSet(pointer0, buffer, bitCount, false)
      ByteBufferBasedBitSet.initializeWith1s(pointer1, buffer, bitCount)
      val s1 = new ByteBufferBasedBitSet(pointer1, buffer, bitCount, true)
      s0.nextSetBit(0) should equal (None)
      s1.nextClearBit(0) should equal (None)
      for (i <- List(0, 31, 32, 63, 64, bitCount - 1)) {
        s0.set(i, true)
        s0.nextSetBit(Random.self.nextInt(bitCount)) should equal (Some(i))
        s0.set(i, false)

        s1.set(i, false)
        s1.nextClearBit(Random.self.nextInt(bitCount)) should equal (Some(i))
        s1.set(i, true)
      }
    }
  }
}
