package co.teapot.thriftbase

import org.apache.thrift.TServiceClient
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.transport.TSocket

trait TeapotThriftClient[ClientClass <:  TServiceClient] {

  val server: String
  val port: Int
  val executor = initExecutor(initProtocol())

  /**
    * If set, the timeout in milliseconds will be passed on to the underlying TCP socket.  Any
    * thrift call which take longer than the timeout will throw an exception rather than keep
    * waiting for a result.
    */
  def timeoutInMillisOption: Option[Int] = None

  def initProtocol(): TProtocol = {
    val transport = timeoutInMillisOption match {
      case Some(timeout) => new TSocket(server, port, timeout)
      case None => new TSocket(server, port)
    }
    transport.open()
    new TBinaryProtocol(transport)
  }

  def initExecutor(protocol: TProtocol): ClientClass

  def getExecutor = executor

  def close(): Unit = {
    getExecutor.getInputProtocol().getTransport().close()
  }
}
