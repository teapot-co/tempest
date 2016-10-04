package co.teapot.tempest.server

import java.io.File

import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.scalatest.{Matchers, FlatSpec}
import scala.concurrent.Future
import scala.sys.process.Process

import scala.concurrent.ExecutionContext.Implicits.global

class TempestDBServerSpec extends FlatSpec with Matchers {

  /**
   * Starts a TempestDBServer and calls a ruby script that
   * exercises the endpoint through the ruby clients in
   * order to test the consistency of the ruby clients as well
   * as the server.
   */

  "A thrift client " should "correctly connect and use the scala endpoint " in {
    // Regenerate the ruby and python packages based on the current source code
    Process("src/main/bash/package_zip.sh").!

    val processor = TempestDBServerTest.getProcessor("example/example_database.yaml")
    val serverTransport = new TServerSocket(10011)
    val serverArgs = new TThreadPoolServer.Args(serverTransport).processor(processor)
    val server = new TThreadPoolServer(serverArgs)
    println("starting server at port " + 10011)

    Future[Unit]{ server.serve() } //Start the server on a new thread

    while(!server.isServing) { Thread.sleep(10) }

    //This notation forwards the process output to stdout
    val ret = Process("ruby -Iruby-package/lib src/test/ruby/tempest_local_test.rb").!
    ret shouldEqual 0

    val pythonResult = Process(
      "src/test/python/tempest_local_test.py",
      cwd=new File("."),
      extraEnv=("PYTHONPATH", "python-package")).!
    pythonResult shouldEqual 0

    server.stop()
  }
}
