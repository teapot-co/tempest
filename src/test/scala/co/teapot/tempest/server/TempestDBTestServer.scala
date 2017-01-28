package co.teapot.tempest.server

import co.teapot.tempest.TempestDBService
import co.teapot.tempest.util.{ConfigLoader, LogUtil}
import co.teapot.thriftbase.TeapotThriftLauncher
import org.apache.thrift.TProcessor

/** Launches a server using small graph and test database. */
object TempestDBTestServer {
  val databaseConfig = ConfigLoader.loadConfig[DatabaseConfig]("src/test/resources/config/database.yaml")
  def getProcessor(configFileName: String): TProcessor = {
    val config = ConfigLoader.loadConfig[TempestDBServerConfig](configFileName)
    val databaseClient = new TempestSQLDatabaseClient(databaseConfig)
    val server = new TempestDBServer( databaseClient, config)
    new TempestDBService.Processor(server)
  }

  def launch(args: Array[String]): Unit = {
    LogUtil.configureLog4j()
    new TeapotThriftLauncher().launch(args ++ Array("-port", "10011"), getProcessor, "src/test/resources/config/tempest.yaml")
  }

  def main(args: Array[String]): Unit = launch(args)
}
