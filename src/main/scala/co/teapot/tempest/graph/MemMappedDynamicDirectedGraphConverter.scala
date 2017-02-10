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

package co.teapot.tempest.graph

import java.io.File
import co.teapot.tempest.io.FileUtil
import co.teapot.tempest.util.{LogUtil, Util, CollectionUtil}
import it.unimi.dsi.fastutil.ints.{IntBigArrayBigList, IntArrayList}

/** The convert method converts a graph from a text file of unsorted edges "<id1><whitespace><id2>"
  * to a binary file which can be efficiently read using MemMappedDynamicDirectedGraph.  This can
  * also be run from the command line; for example:
  * sbt assembly
  * printf "1 2\n3 4\n1 3" > input_graph.txt
  * java -cp target/scala-2.11/tempest<current_version>.jar \
  * co.teapot.tempest.graph.MemMappedDynamicDirectedGraphConverter input_graph.txt output_graph.dat
  *
  * Then in scala code, MemMappedDynamicDirectedGraph("output_graph.dat") will efficiently read the
  * graph and allow for further nodes and edges to be added.
  */
// TODO: Is there some way to unify this code and MemoryMappedDirectedGraphConverter?
object MemMappedDynamicDirectedGraphConverter {

  /**
    * Converts a text file of lines "<id1><whitespace><id2>" into a binary memory mapped graph.
    * Any duplicate edges are included in the output.  Empty lines are ignored.
    * Progress is logged using the given function.
    */
  def convert(edgeListFile: File,
              outputGraphFile: File,
              log: String => Unit = System.err.println): Unit = {
    val graph = new MemMappedDynamicDirectedGraph(outputGraphFile, syncAllWrites=false)
    val outDegrees = new IntArrayList()
    val inDegrees = new IntArrayList()

    Util.logWithRunningTime(log, "first pass", printAtStart = true) {
      FileUtil.forEachIntPair(edgeListFile, log, linesPerMessage = 1000000) { (id1, id2) =>
        if (id1 >= outDegrees.size) {
          outDegrees.addAll(IntArrayList.wrap(new Array[Int](id1 + 1 - outDegrees.size)))
        }
        if (id2 >= inDegrees.size) {
          inDegrees.addAll(IntArrayList.wrap(new Array[Int](id2 + 1 - inDegrees.size)))
        }
        outDegrees.set(id1, outDegrees.get(id1) + 1)
        inDegrees.set(id2, inDegrees.get(id2) + 1)
      }
    }
    val maxNodeId = math.max(outDegrees.size - 1, inDegrees.size - 1)

    Util.logWithRunningTime(log, "Allocating neighbor arrays", printAtStart = true) {
      println(s"Allocating space for $maxNodeId nodes")
      graph.ensureValidId(maxNodeId)
      for (id1 <- 0 until outDegrees.size) {
        if (id1 < 5) log(s"Reserving ${outDegrees.get(id1)} out-neighbors for $id1")
        if (outDegrees.get(id1) > 0) {
          graph.setOutDegreeCapacity(id1.toInt, outDegrees.get(id1))
        }
      }
      for (id2 <- 0 until inDegrees.size) {
        if (id2 < 5) log(s"Reserving ${inDegrees.get(id2)} in-neighbors for $id2")
        if (inDegrees.get(id2) > 0) {
          graph.setInDegreeCapacity(id2.toInt, inDegrees.get(id2))
        }
      }
    }

    Util.logWithRunningTime(log, "2nd pass: Adding edges", printAtStart = true) {
      FileUtil.forEachIntPair(edgeListFile, log, linesPerMessage = 1000000) { (id1, id2) =>
        //log(s"adding edge $id1, $id2")
        graph.addEdge(id1, id2)
      }
    }

    Util.logWithRunningTime(log, "Syncing graph to disk") {
      graph.syncToDisk()
    }

    log(s"read ${graph.edgeCount} total edges (including any duplicates)")
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: MemMappedDynamicDirectedGraphConverter " +
        "<edge list filename> <binary output filename> [debug]")
      System.exit(1)
    }
    val inputFile = new File(args(0))
    val outputFile = new File(args(1))
    val debugMode = args.length > 2
    if (debugMode)
      LogUtil.configureConsoleLog("")
    if (outputFile.exists()) {
      println(s"Output file ${outputFile} already exists. If desired, delete it and retry.")
      System.exit(1)
    }
    convert(inputFile, outputFile)
  }
}
