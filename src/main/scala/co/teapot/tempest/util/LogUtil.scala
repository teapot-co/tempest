package co.teapot.tempest.util

import com.twitter.logging.{ConsoleHandler, Logger, LoggerFactory}
import org.apache.log4j.{BasicConfigurator, DailyRollingFileAppender, PatternLayout, Level, Logger => Logger4j}

object LogUtil {
  def configureLog4j(): Unit = {
    // Thrift uses log4j, which complains if logging is not configured
    BasicConfigurator.configure() // write logs to stdout as well
    Logger4j.getRootLogger.setLevel(Level.INFO)
    val layout = new PatternLayout("%-5p %d [%t]: %m%n")
    val appender = new DailyRollingFileAppender(layout, "main_log4j.log", "'.'yyyy-MM-dd")
    appender.setThreshold(Level.INFO)
    BasicConfigurator.configure(appender)
  }

  def configureConsoleLog(nodeName: String): Unit = {
    LoggerFactory(node = nodeName,
      level = Some(Logger.ALL),
      handlers = List(ConsoleHandler()),
      useParents = false
    )()
  }
}
