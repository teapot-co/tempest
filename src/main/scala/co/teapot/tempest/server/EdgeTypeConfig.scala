package co.teapot.tempest.server

import scala.beans.BeanProperty

class EdgeTypeConfig {
  @BeanProperty var csvFile: String = null
  @BeanProperty var nodeType1: String = null
  @BeanProperty var nodeType2: String = null
  @BeanProperty var nodeIdentifierField1: String = null
  @BeanProperty var nodeIdentifierField2: String = null
}
