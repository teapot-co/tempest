package co.teapot.util

import org.scalatest.{Matchers, FlatSpec}

class UtilSpec extends FlatSpec with Matchers {
  "Util" should "find next leading 2 bit numbers correctly" in {
    Util.nextLeadingTwoBitNumber(0) should equal (0)
    Util.nextLeadingTwoBitNumber(1) should equal (1)
    Util.nextLeadingTwoBitNumber(2) should equal (2)
    Util.nextLeadingTwoBitNumber(3) should equal (3)
    Util.nextLeadingTwoBitNumber(4) should equal (4)
    Util.nextLeadingTwoBitNumber(5) should equal (6)
    Util.nextLeadingTwoBitNumber(6) should equal (6)
    Util.nextLeadingTwoBitNumber(7) should equal (8)
    Util.nextLeadingTwoBitNumber(8) should equal (8)
    Util.nextLeadingTwoBitNumber(9) should equal (12)
    Util.nextLeadingTwoBitNumber(10) should equal (12)
    Util.nextLeadingTwoBitNumber(11) should equal (12)
    Util.nextLeadingTwoBitNumber(12) should equal (12)
    Util.nextLeadingTwoBitNumber(13) should equal (16)
    Util.nextLeadingTwoBitNumber((1 << 30) + 1) should equal ((1 << 30) + (1 << 29))

    // Check correctness on Longs
    Util.nextLeadingTwoBitNumber((1L << 31) + 1) should equal ((1L << 31) + (1L << 30))
    Util.nextLeadingTwoBitNumber((1L << 40) + (1L << 39) + 1) should equal (1L << 41)
  }

  it should " support longRange " in {
    Util.longRange(1L << 42, (1L << 42) + 3).toSeq should contain theSameElementsInOrderAs Seq(
      (1L << 42) + 0, (1L << 42) + 1, (1L << 42) + 2
    )
  }
}
