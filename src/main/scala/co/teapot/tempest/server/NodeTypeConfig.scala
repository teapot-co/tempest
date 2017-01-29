package co.teapot.tempest.server

import scala.beans.BeanProperty
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class NodeTypeConfig {
  @BeanProperty var csvFile: String = null
  @BeanProperty var nodeAttributes: util.List[util.Map[String, String]] = null

  def attributeTypePairs: Seq[(String, String)] = {
    val result = new ArrayBuffer[(String, String)]()
    for (nodeTypePairMap <- nodeAttributes.asScala) {
      for (nodeTypePair <- nodeTypePairMap.asScala) {
        result += nodeTypePair
      }
    }
    result
  }

  def attributeSet: Set[String] = {
    (attributeTypePairs map (_._1)).toSet
  }
}
