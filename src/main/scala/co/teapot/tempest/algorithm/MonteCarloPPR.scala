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

import co.teapot.tempest.graph.{DirectedGraph, EdgeDir}
import co.teapot.tempest.MonteCarloPageRankParams
import co.teapot.tempest.util.CollectionUtil

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random


/**
  * These methods perform Monte Carlo walks from a list of seed nodes (specified by their ids).
  * They are generalized in a few different ways:
  *   - LengthConstraint specifies that the walk have even, odd, or any length.
  *   - firstStepDirection can specify an out-neighbor or in-neighbor is followed first.
  *   - params.alternating specifies whether the walk keeps following out-neighbors (PPR) or alternates between out
  *     and in-neighbors (SALSA)
  *  As examples, on Twitter if you have a tweet producer like Obama and want to find similar tweet producers,
  *     you likely want to use an even-length alternating walk, starting with EdgeDir.In (to first find followers of Obama).
  *  On Twitter if you have a tweet consumer, say Alice, and want to find similar tweet consumers,
  *     you likely want to use an even-length alternating walk, starting with EdgeDir.Out (to first find who Alice follows).
  *  On Twitter if you have a tweet consumer, say Alice, and want to recommend who-to-follow,
  *     you likely want to use an odd-length alternating walk, starting with EdgeDir.Out (to first find who Alice follows).
  *  On Twitter, if we want to find a mega-celebrity, who other celebrities follow, you likely want to use a non-alternating
  *     walk of unconstrained length.
  *  On a user-item bipartite graphs, if you want to find similar items to a given item, see the tweet producer case above.
  *  On a user-item bipartite graphs, if you want to find similar users to a given user, see the tweet consumer case above.
  *  On a user-item bipartite graphs, if you want to recommend items to a given user, see the who-to-follow case above.
  *
  *  Background reading: "WTF: the who to follow service at Twitter" by Gupta, Goel, Lin, Sharma, Wnag, Zadeh.
  */
object MonteCarloPPR {
  def estimatePPR(graph: DirectedGraph,
                  startIds: IndexedSeq[Int],
                  firstStepDirection: EdgeDir,
                  lengthConstraint: LengthConstraint,
                  params: MonteCarloPageRankParams,
                  random: Random = new Random()):
  collection.Map[Int, Double] = {
    val pprs = CollectionUtil.efficientIntDoubleMapWithDefault0()
    def randomStart(): Int = startIds(random.nextInt(startIds.size))

    var currentDirection = firstStepDirection
    var currentLength = 0
    var v = randomStart()

    for (stepIndex <- 0 until params.getNumSteps) {
      // Make sure our walk length is acceptable (e.g. even or odd according to lengthConstraint).  If not, don't record
      // that we visited this node.
      if (lengthConstraint.isAllowed(currentLength)) {
        pprs(v) += 1.0 / params.getNumSteps
      }

      val vDegree = graph.degree(v, currentDirection)
      if (random.nextDouble() < params.getResetProbability || vDegree == 0) {
        v = randomStart()
        currentDirection = firstStepDirection
        currentLength = 0
      } else {
        v = graph.neighbor(v, random.nextInt(vDegree), currentDirection)
        if (params.alternatingWalk) {
          currentDirection = currentDirection.flip
        }
        currentLength += 1
      }
    }
    filterPPRResults(pprs, params)
  }

  /** See estimatePPR for an explanation of the arguments. */
  def estimatePPRParallel(graph: DirectedGraph,
                          startIds: IndexedSeq[Int],
                          firstStepDirection: EdgeDir,
                          lengthConstraint: LengthConstraint,
                          params: MonteCarloPageRankParams,
                          threadCount: Int):
  collection.Map[Int, Double] = {
    val paramsForThread = params.deepCopy()
    paramsForThread.setNumSteps(params.getNumSteps / threadCount) // Each thread will do it's share of the steps.
    paramsForThread.setMinReportedVisits(0) // Threads shouldn't filter before counts have been averaged.
    val pprMapFutures = (0 until threadCount) map { i =>
      Future[collection.Map[Int, Double]] {
        estimatePPR(graph, startIds, firstStepDirection, lengthConstraint, paramsForThread)
      }
    }

    val pprMaps = Await.result(Future.sequence(pprMapFutures), Duration.Inf)
    val pprs = CollectionUtil.mean(pprMaps)

    filterPPRResults(pprs, params)
  }

  /** Applies the isSetMinReportedVisits and maxResultCount constraints of the given params. */
  def filterPPRResults(pprs: collection.Map[Int, Double], params: MonteCarloPageRankParams): collection.Map[Int, Double] = {
    val prunedPPRs = if (params.isSetMinReportedVisits) {
      pprs filter {
        // Subtracting 0.5 is just to avoid floating point exactness issues
        case (id, prob) => prob > (params.getMinReportedVisits - 0.5) / params.getNumSteps
      }
    } else {
      pprs
    }
    if (params.isSetMaxResultCount) {
      (prunedPPRs.toIndexedSeq sortBy (-_._2) take params.maxResultCount).toMap
    } else {
      prunedPPRs
    }
  }


  sealed abstract class LengthConstraint {
    def isAllowed(length: Int): Boolean
  }
  case object EvenLength extends LengthConstraint {
    def isAllowed(length: Int): Boolean = length % 2 == 0
  }
  case object OddLength extends LengthConstraint {
    def isAllowed(length: Int): Boolean = length % 2 == 1
  }
  case object AnyLength extends LengthConstraint {
    def isAllowed(length: Int): Boolean = true
  }
}
