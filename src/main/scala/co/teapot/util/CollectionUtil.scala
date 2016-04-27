package co.teapot.util

import net.openhft.koloboke.collect.map.hash.{HashIntIntMaps, HashIntDoubleMaps, HashIntFloatMaps}

import scala.collection.mutable
import scala.collection.JavaConverters._

object CollectionUtil {
  def efficientIntFloatMap(): mutable.Map[Int, Float] =
  // For some reason, asInstanceOf is necessary to convert java.lang.Integer to scala.Int
    HashIntFloatMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Float]]

  def efficientIntDoubleMap(): mutable.Map[Int, Double] =
  // For some reason, asInstanceOf is necessary to convert java.lang.Integer to scala.Int
    HashIntDoubleMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Double]]

  def efficientIntIntMap(): mutable.Map[Int, Int] =
    HashIntIntMaps.newMutableMap().asScala.asInstanceOf[mutable.Map[Int, Int]]
}
