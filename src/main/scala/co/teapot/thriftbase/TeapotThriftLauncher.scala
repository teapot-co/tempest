package co.teapot.thriftbase

import com.twitter.app.Flags
import org.apache.thrift.TProcessor
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket

class TeapotThriftLauncher {

  def createServer(getProcessor: String => TProcessor, configFile: String, port: Int)
  : TThreadPoolServer = {
    val processor = getProcessor(configFile)
    val serverTransport = new TServerSocket(port)
    val serverArgs = new TThreadPoolServer.Args(serverTransport).processor(processor)
    new TThreadPoolServer(serverArgs)
  }

  def launch(args: Array[String], getProcessor: String => TProcessor,
             defaultConfFilename: String = ""): Unit = {
    try {
      val flags = new Flags("The Teapot Thrift Launcher")
      val configFileFlag = flags[String]("conf", defaultConfFilename, "Thrift server specific config file")
      val portFlag = flags[Int]("port", 10001, "port to listen on")
      val helpFlag = flags("h", false, "Print usage")
      flags.parse(args)

      if (helpFlag()) {
        println(flags.usage)
      } else {
        val server = createServer(getProcessor, configFileFlag(), portFlag())
        println("starting server at port " + portFlag())
        server.serve()
      }
    } catch {
      case x: Exception => x.printStackTrace()
    }
  }
}
