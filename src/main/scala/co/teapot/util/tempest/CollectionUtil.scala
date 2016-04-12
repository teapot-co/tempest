package co.teapot.util.tempest

import java.util
import scala.collection.JavaConverters._

object CollectionUtil {
  def toScala(xs: util.List[Integer]): collection.IndexedSeq[Int] =
    xs.asScala.toIndexedSeq map { _.toInt}
}
