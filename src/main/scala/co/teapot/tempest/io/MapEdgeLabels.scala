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

import java.io.{BufferedWriter, FileWriter}

import net.openhft.koloboke.collect.map.hash.HashObjIntMaps

/**
  * Maps a file of edges with application-specific labels to a file of edges with numeric tempest ids.
  * Given
  *   - a csv file mapping node label to tempest_id (with lines of the form "<label>,<tempest_id>"),
  *   - a file with edges (with lines of the form "<label1>,<label2>"), and
  *   - an output file,
  * this object maps the given edges to tempest ids, and creates the output file with lines of the form
  * "<tempest_id1> <tempest_id2>".
  */
object MapEdgeLabels {
  def main(args: Array[String]): Unit = {
    if (args.size != 3) {
      println("usage: MapEdgeLabels <id mapping file> <input edge filename> " +
        "<output edge filename>")
      System.exit(1)
    }
    val idMapFilename = args(0)
    val edgeInputFilename = args(1)
    val edgeOutputFilename = args(2)

    val labelToTempestId = HashObjIntMaps.newMutableMap[String]()
    //val labelToTempestId = new Object2IntOpenHashMap[String]()
    FileUtil.foreachCSVLine(idMapFilename, 2, ",") { tokens: Array[String] =>
      labelToTempestId.put(tokens(0), tokens(1).toInt)
    }

    val bufferedOut = new BufferedWriter(new FileWriter(edgeOutputFilename))
    var discardedEdgeCount = 0L
    FileUtil.foreachCSVLine(edgeInputFilename, 2, ",") { tokens: Array[String] =>
      val label1 = tokens(0)
      val label2 = tokens(1)
      if (labelToTempestId.containsKey(label1) && labelToTempestId.containsKey(label2)) {
        bufferedOut.write(s"${labelToTempestId.getInt(label1)} ${labelToTempestId.getInt(label2)}\n")
      } else {
        discardedEdgeCount += 1
      }
    }
    bufferedOut.flush()
    if (discardedEdgeCount > 0) {
      System.err.println(s"Discarded $discardedEdgeCount edges from $edgeInputFilename for not having " +
        "person ids")
    }
  }
}
