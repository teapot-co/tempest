package co.teapot.tempest.util

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

  def loadConfig[T:ClassTag](configFile: File): T = {
    if (configFile.exists) {
      loadConfig(scala.io.Source.fromFile(configFile))
    } else {
      throw new Exception("Configuration file not found: " + configFile.getCanonicalPath)
    }
  }

  def loadConfig[T:ClassTag](configFileName: String): T = {
    loadConfig(new File(configFileName))
  }
}
