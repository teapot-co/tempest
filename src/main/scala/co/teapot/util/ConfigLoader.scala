package co.teapot.util

import java.io.File

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.io.BufferedSource
import scala.reflect.ClassTag

/*
 * Loads yaml config files.
 */

object ConfigLoader {
  def loadConfig[T:ClassTag](source: BufferedSource): T = {
    val yaml = new Yaml(new Constructor(implicitly[ClassTag[T]].runtimeClass))
    yaml.load(source.getLines().mkString("\n")).asInstanceOf[T]
  }

  def loadConfig[T:ClassTag](configFileName: String): T = {
    val file = new File(configFileName)
    if (file.exists) {
      loadConfig(scala.io.Source.fromFile(file))
    } else {
      throw new Exception("Configuration file not found: " + configFileName)
    }
  }
}
