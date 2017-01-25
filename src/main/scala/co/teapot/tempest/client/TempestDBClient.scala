/*
 * Copyright 2016 Teapot, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package co.teapot.tempest.client

import co.teapot.tempest.{Node, TempestDBService}
import co.teapot.thriftbase.TeapotThriftClient
import org.apache.thrift.protocol.TProtocol

class TempestDBClient(val server: String,
                      val port: Int,
                      override val timeoutInMillisOption: Option[Int] = None
                     ) extends TeapotThriftClient[TempestDBService.Client] {
  def initExecutor(protocol: TProtocol) = {
    new TempestDBService.Client(protocol)
  }
}

// Example code for using TempestClient
object TempestDBClient {
  def main(args: Array[String]) = {
    val client = new TempestDBClient("localhost", 10001)
    println ("Node  1 has indegree " +
      client.getExecutor.inDegree("has_read", new Node("users", "alice")) +
      " and outdegree " + client.getExecutor.outDegree("has_read", new Node("users", "alice")))
    client.close()
  }
}
