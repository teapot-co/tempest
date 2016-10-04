package co.teapot.util

import java.lang

object Util {
  /** Returns ceilng(x / y). Assumes x >= 0, y > 0. */
  def divideRoundingUp(x: Long, y: Long): Long =
    (x + y - 1) / y
  /** Returns ceilng(x / y). Assumes x >= 0, y > 0. */
  def divideRoundingUp(x: Int, y: Int): Int =
    (x + y - 1) / y

  /** Returns the smallest integer >= x which in binary has either a single 1 or two 1s together.
    * Equivalently, returns the smallest integer >= x of the form 2**k or 3/2 * 2**k for some k.*/
  def nextLeadingTwoBitNumber(x: Int): Int = {
    val y = Integer.highestOneBit(x)
    if (y >= x) { // If x is a power of 2
      y
    } else if (y + (y >> 1) >= x) { // try 3/2 * (a power of two)
      y + (y >> 1)
    } else { // return next power of two
      assert(y << 1 >= x)
      y << 1
    }
  }

  def nextLeadingTwoBitNumber(x: Long): Long = {
    val y = lang.Long.highestOneBit(x)
    if (y >= x) { // If x is a power of 2
      y
    } else if (y + (y >> 1) >= x) { // try 3/2 * (a power of two)
      y + (y >> 1)
    } else { // return next power of two
      assert(y << 1 >= x)
      y << 1
    }
  }

  // Just using "start until long" fails if there are more than Integer.MAX_VALUE elements.
  def longRange(start: Long, end: Long): Iterator[Long] = new Iterator[Long]() {
    var current = start
    override def hasNext: Boolean = current < end
    override def next(): Long = {
      current += 1
      current - 1
    }
  }

  def logWithRunningTime[A](log: String => Unit, message: String, printAtStart: Boolean = false)
                           (f: => A): A  = {
    if (printAtStart)
      log(s"starting $message")
    val startTime = System.currentTimeMillis
    val result = f
    val millisPerSecond = 1000.0
    val timeTaken = (System.currentTimeMillis - startTime) / millisPerSecond
    log(s"$message took $timeTaken s")
    result
  }
}
