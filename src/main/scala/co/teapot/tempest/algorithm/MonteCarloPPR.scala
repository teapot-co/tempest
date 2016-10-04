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

package co.teapot.tempest.algorithm

import co.teapot.tempest.graph.DirectedGraph
import co.teapot.tempest.MonteCarloPageRankParams
import co.teapot.tempest.util.CollectionUtil

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

/**
  * Performing Monte Carlo walks from a list of seed nodes (specified by their ids)
  */
object MonteCarloPPR {
  def estimatePPR(graph: DirectedGraph, startIds: IndexedSeq[Int], params: MonteCarloPageRankParams,
                  random: Random = new Random()):
  collection.Map[Int, Double] = {
    val pprs = CollectionUtil.efficientIntDoubleMapWithDefault0()
    def randomStart(): Int = startIds(random.nextInt(startIds.size))
    var v = randomStart()
    for (stepIndex <- 0 until params.getNumSteps) {
      pprs(v) += 1.0 / params.getNumSteps
      if (random.nextDouble() < params.getResetProbability) {
        v = randomStart()
      } else {
        val vDegree = graph.outDegree(v)
        if (vDegree > 0)
          v = graph.outNeighbor(v, random.nextInt(vDegree))
        else
          v = randomStart()
      }
    }
    if (params.isSetMinReportedVisits) {
      pprs retain {
        // Subtracting 0.5 is just to avoid floating point exactness issues
        case (id, prob) => prob > (params.getMinReportedVisits - 0.5) / params.getNumSteps
      }
    }
    if (params.isSetMaxResultCount) {
      (pprs.toIndexedSeq sortBy (-_._2) take params.maxResultCount).toMap
    } else {
      pprs
    }
  }

  def estimatePPRParallel(graph: DirectedGraph, startIds: IndexedSeq[Int], params:
  MonteCarloPageRankParams, threadCount: Int):
  collection.Map[Int, Double] = {
    params.setNumSteps(params.getNumSteps / threadCount)
    val pprMapFutures = (0 until threadCount) map { i =>
      Future[collection.Map[Int, Double]] {
        estimatePPR(graph, startIds, params)
      }
    }

    val pprMaps = Await.result(Future.sequence(pprMapFutures), Duration.Inf)
    CollectionUtil.mean(pprMaps)
  }
}
