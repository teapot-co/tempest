package co.teapot.io

import org.scalatest.{Matchers, FlatSpec}

import scala.io.Source

class MapEdgeLabelsSpec extends FlatSpec with Matchers {
  "Map edges" should "work correctly on an example" in {
    val idMapFile = FileUtil.stringToTemporaryFile("alice,0\ncarol,2\nbob,1")
    val edgeFile = FileUtil.stringToTemporaryFile("alice,bob\ncarol,bob")
    val outputFile = FileUtil.stringToTemporaryFile("")
    MapEdgeLabels.main(Array(
      idMapFile.getAbsolutePath,
      edgeFile.getAbsolutePath,
      outputFile.getAbsolutePath))
    Source.fromFile(outputFile).mkString shouldEqual ("0 1\n2 1\n")
  }
}
