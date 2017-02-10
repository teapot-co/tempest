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

package co.teapot.tempest.io

import java.io.{File, FileWriter}
import java.util.regex.Pattern

import scala.io.Source

object FileUtil {
  /**
    * Splits each line into nColumns columns and calls the given function on each line.
    * Logs an error if a nonempty line has fewer columns than expected. Only the
    * last field may contain the separator, and no field may contain newlines.
    */
  def foreachCSVLine(filename: String, nColumns: Int, separatorRegex: String = ",",
                     log: String => Unit = System.err.println, logProgress: Boolean = true)
                    (f: Array[String] => _) {
    foreachLine(new File(filename), log, logProgress) { line =>
      val parts = line.split(separatorRegex, nColumns)
      if (parts.size != nColumns) {
        System.err.println(s"""csv line has fewer than ${nColumns - 1} \"${separatorRegex}\"s: $line""")
      } else {
        f(parts)
      }
    }
  }

  def forEachLongPair(intPairFile: File, log: String => Unit, logProgress: Boolean = true, linesPerMessage: Int = 1000000)
                            (f: (Long, Long) => Unit): Unit = {
    val linePattern = Pattern.compile(raw"(-?\d+)\s+(-?\d+)") // Temporarily allow negative ids TODO(undo)
    foreachLine(intPairFile, log, logProgress, linesPerMessage) { line =>
      val matcher = linePattern.matcher(line)
      if (!matcher.matches()) {
        log("invalid line: " + line)
      } else {
        val u = matcher.group(1).toLong // Groups are 1-indexed
        val v = matcher.group(2).toLong
        f(if (u >= 0) u else u + (1L << 32), if (v >= 0) v else v + (1L << 32)) // TODO: Remove hacky support for negative numbers
      }
    }
  }

  def forEachIntPair(intPairFile: File, log: String => Unit, logProgress: Boolean = true, linesPerMessage: Int = 1000000)
                     (f: (Int, Int) => Unit): Unit = {
    val linePattern = Pattern.compile(raw"(\d+)\s+(\d+)")
    foreachLine(intPairFile, log, logProgress, linesPerMessage) { line =>
      val matcher = linePattern.matcher(line)
      if (!matcher.matches()) {
        log("invalid line: " + line)
      } else {
        val u = matcher.group(1).toInt // Groups are 1-indexed
        val v = matcher.group(2).toInt
        f(u, v)
      }
    }
  }
  /** Iterates over nonempty lines in the given file, optionally logging progress periodically using the
    * given log function.
    */
  def foreachLine(file: File, log: String => Unit, logProgress: Boolean = true, linesPerMessage: Int = 1000000)
                 (f: String => Unit): Unit = {
    var lineCount = 0L
    val startTime = System.currentTimeMillis
    var previousUpdateTime = startTime
    for (line <- Source.fromFile(file).getLines()
         if line.nonEmpty) {
      f(line)
      lineCount += 1
      if (lineCount % linesPerMessage == 0) {
        val linesPerSec = linesPerMessage * 1000L / (System.currentTimeMillis() - previousUpdateTime)
        val AverageLinesPerSec = lineCount * 1000L / (System.currentTimeMillis() - startTime)

        log(s"Read $lineCount lines. Reading $linesPerSec lines per second (total average $AverageLinesPerSec).")
        previousUpdateTime = System.currentTimeMillis
      }
    }
  }

  def stringToTemporaryFile(contents: String): File = {
    val f = File.createTempFile("temp", ".txt")
    val writer = new FileWriter(f)
    writer.write(contents)
    writer.close()
    f.deleteOnExit()
    f
  }
}
