package co.teapot.util

import java.{lang, util}

import co.teapot.tempest.DegreeFilterTypes
import net.openhft.koloboke.collect.map.hash.{HashIntDoubleMaps, HashIntFloatMaps}
import net.openhft.koloboke.collect.set.hash.{HashIntSet, HashIntSets}

import scala.collection.JavaConverters._
import scala.collection.mutable

object CollectionUtil {
  def toJava(xs: Seq[Long]): util.List[lang.Long] =
    (xs map (new lang.Long(_))).asJava

  def toJava(m: collection.Map[Long, Double]): util.Map[lang.Long, lang.Double] = {
    m.asJava.asInstanceOf[util.Map[lang.Long, lang.Double]]
  }

  // Note: Naming this toJava doesn't work. Is there a more elegant way of doing this?
  def toJavaLongStringMap(m: collection.Map[Long, String]): util.Map[lang.Long, String] = {
    m.asJava.asInstanceOf[util.Map[lang.Long, String]]
  }

  def toScala(xs: util.List[lang.Long]): collection.IndexedSeq[Long] =
    xs.asScala.toIndexedSeq map { _.toLong }

  def integersToScala(xs: util.List[Integer]): collection.IndexedSeq[Int] =
    xs.asScala.toIndexedSeq map { _.toInt }

  def toScala(degreeFilterMap: java.util.Map[DegreeFilterTypes, Integer]): collection.Map[DegreeFilterTypes, Int] =
    degreeFilterMap.asScala.map{case (k: DegreeFilterTypes, v:Integer) => (k, v.toInt)}

  def toScala(xs: Iterable[lang.Long]): collection.IndexedSeq[Long] =
    (xs map { _.toLong}).toIndexedSeq

  def toHashIntSet(xs: Seq[Int]): HashIntSet = {
    val result = HashIntSets.newMutableSet(xs.size)
    for (u <- xs)
      result.add(u)
    result
  }

  /**
    * Returns a map whose value at each key is the mean of the values at that key of the given maps.
    */
  def mean(maps: Seq[collection.Map[Int, Double]] ): collection.Map[Int, Double] = {
    val result = efficientIntDoubleMapWithDefault0()
    for (map <- maps) {
      for ((k, v) <- map) {
        result(k) = result(k) + v / maps.size
      }
    }
    result
  }

  def efficientIntFloatMap(): mutable.Map[Int, Float] =
  // For some reason, asInstanceOf is still necessary to convert java.lang.Integer to scala.Int
    HashIntFloatMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Float]]
  // In the future, this might be replaced with a more efficient version
  def efficientIntFloatMapWithDefault0(): mutable.Map[Int, Float] =
    efficientIntFloatMap().withDefaultValue(0.0f)
  def efficientIntDoubleMap(): mutable.Map[Int, Double] =
  // For some reason, asInstanceOf is still necessary to convert java.lang.Integer to scala.Int
    HashIntDoubleMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Double]]
  def efficientIntDoubleMapWithDefault0(): mutable.Map[Int, Double] =
    efficientIntDoubleMap().withDefaultValue(0.0)

  def longRange(startInclusive: Long, endExclusive: Long): Iterator[Long] = new Iterator[Long] {
    var i: Long = startInclusive
    override def hasNext: Boolean = i < endExclusive

    override def next(): Long = {
      i += 1
      i - 1
    }
  }
}
