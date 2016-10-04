package co.teapot.server

import java.io.File

import co.teapot.graph.MemoryMappedDirectedGraphConverter
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.TServerSocket
import org.scalatest.{Matchers, FlatSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.sys.process.Process

class TempestServerSpec extends FlatSpec with Matchers {

  /**
    * Starts a TempestDBServer and calls a python script that
    * exercises the endpoint through the client in
    * order to test the consistency of the client as well
    * as the server.
    */

  "A thrift client " should "correctly connect and use the scala endpoint " in {
    // Convert test graph from text to binary
    val tempBinaryFile = File.createTempFile("graph1", ".bin")
    new MemoryMappedDirectedGraphConverter(
      new File("src/test/resources/test_graph.txt"),
      tempBinaryFile
    ).convert()

    val processor = TempestServer.getProcessor(tempBinaryFile.getAbsolutePath)
    val serverTransport = new TServerSocket(10012)
    val serverArgs = new TThreadPoolServer.Args(serverTransport).processor(processor)
    val server = new TThreadPoolServer(serverArgs)
    println("starting server at port " + 10012)
    Future[Unit]{ server.serve() } //Start the server on a new thread

    // Wait for server to start
    while(!server.isServing) { Thread.sleep(10) }

    val pythonResult = Process(
      "src/test/python/tempest_graph_local_test.py",
      cwd=new File("."),
      extraEnv=("PYTHONPATH", "python-package-graph")).! // .! forwards output to standard out
    pythonResult shouldEqual 0

    server.stop()
  }
}
