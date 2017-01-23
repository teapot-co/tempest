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

import net.openhft.koloboke.collect.set.hash.{HashIntSet, HashIntSets}
import scala.collection.JavaConverters._

object DirectedGraphAlgorithms {
  /** Returns all distinct node ids k steps from the source node.  For example, k=0 will return
    just the source, and k=1 will return its out-neighbors. Only out-edges (not in-edges) are
    followed.  */
  def kStepOutNeighbors(graph: DirectedGraph, source: Int, k: Int, alternating: Boolean): HashIntSet = {
    if (k == 0)
      HashIntSets.newMutableSetOf(source)
    else {
      val kMinus1Neighborhood = kStepOutNeighbors(graph, source, k - 1, alternating)
      val result = HashIntSets.newMutableSet()

      for (u <- kMinus1Neighborhood.asScala) {
        val neighbors = if (!alternating || (k % 2 == 1))
          graph.outNeighbors(u)
        else
          graph.inNeighbors(u)
        for (v <- neighbors) {
          result.add(v)
        }
      }
      result
    }
  }
}
