package co.teapot.experiments

import co.teapot.graph.{DirectedGraph, GraphGenerator}
import experiments.PPRBigArrayPowerIteration
import org.scalatest.{Matchers, FlatSpec}
import com.twitter.logging.Logger

/*
 * Testing PPRBigArrayPowerIteration on a complete graph and a graph with sinks
 */

class PPRBigArrayPowerIterationSpec extends FlatSpec with Matchers {

  val TOL = 0.001f
  val numIterations = 50
  val resetProbability = 0.2.toFloat
  
  val pprGlobal = PPRBigArrayPowerIteration.calculate(GraphGenerator.completeGraph(7),
    List(0L, 1L, 2L, 3L, 4L, 5L, 6L), numIterations, resetProbability)
  val diffsGlobal = (0 until 7).map(nodeId => Math.abs(pprGlobal.get(nodeId) * 7 - 1.0)).sum
  "The PPR computation with every node in the seed set" should "work correctly on complete graphs" in {
    diffsGlobal should equal(0.0 +- (TOL*7))
  }

  val pprLocal = PPRBigArrayPowerIteration.calculate(GraphGenerator.completeGraph(7),
    List(1L), numIterations, resetProbability)
  val expectedScoreLocal = 2.0/6.8 // p = 0.2 + 0.8 * (1-p)/6 => 6.8p = 2
  "The PPR computation with one node in the seed set" should "work correctly on complete graphs" in {
    pprLocal.get(1).toDouble should equal(expectedScoreLocal  +- TOL)
  }

  val graphDangling = DirectedGraph(0 -> 1)
  val pprLocalDanglingNodes = PPRBigArrayPowerIteration.calculate(graphDangling,
    List(0L), numIterations, resetProbability)

  val expectedScoreDangling = 1.0/1.8 // p = 0.2*p + (1-p) => p = 1.0/1.8
  "The PPR computation with dangling nodes" should "work correctly on two node graph" in {
    pprLocalDanglingNodes.get(0).toDouble should equal (expectedScoreDangling  +- TOL)
  }
}
