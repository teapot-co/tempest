package co.teapot.util

import org.scalatest.{Matchers, FlatSpec}

class CollectionUtilSpec extends FlatSpec with Matchers {
  "CollectionUtil" should "support LongRange" in {
    CollectionUtil.longRange(1L, 3L).toSeq should contain theSameElementsAs (Seq(1, 2))
  }
}
