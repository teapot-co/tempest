/*
 * Copyright 2017 Stripe
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


package co.teapot.tempest.algorithm

import co.teapot.tempest.MonteCarloPageRankParams
import co.teapot.tempest.typedgraph.{IntNode, TypedGraph}

import scala.collection.mutable
import scala.util.Random

object MonteCarloPPRTyped {
  def estimatePPR(graph: TypedGraph,
                  seeds: IndexedSeq[IntNode],
                  params: MonteCarloPageRankParams,
                  random: Random = new Random()):
  collection.Map[IntNode, Double] = {
    // Note: For efficiency, a future version could group nodes by type, and implement a Map[IntNode, Double] using
    // mutable.AnyRefMap[String, HashIntIntMap]
    val pprs = new mutable.AnyRefMap[IntNode, Double]().withDefaultValue(0.0)
    def randomStart(): IntNode = seeds(random.nextInt(seeds.size))

    var v = randomStart()

    for (stepIndex <- 0 until params.getNumSteps) {
      pprs(v) += 1.0 / params.getNumSteps

      val vDegree = graph.degree(v)
      if (random.nextDouble() < params.getResetProbability || vDegree == 0) {
        v = randomStart()
      } else {
        v = graph.randomNeighbor(v, random)
      }
    }
    MonteCarloPPR.filterPPRResults(pprs, params)
  }
}
