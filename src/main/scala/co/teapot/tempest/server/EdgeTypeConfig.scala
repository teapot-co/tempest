package co.teapot.tempest.server

import scala.beans.BeanProperty

class EdgeTypeConfig {
  @BeanProperty var csvFile: String = null
  @BeanProperty var sourceNodeType: String = null
  @BeanProperty var targetNodeType: String = null
  @BeanProperty var sourceNodeIdentifierField: String = null
  @BeanProperty var targetNodeIdentifierField: String = null
}
