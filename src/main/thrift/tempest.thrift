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

namespace java co.teapot.graph
namespace py tempest_graph

typedef i32 int
typedef i64 long

service TempestService {
  int outDegree(1:int id)
  int inDegree(1:int id)

  list<int> outNeighbors(1:int id)
  list<int> inNeighbors(1:int id)

  /* Returns the ith out-neighbor of the given node.
     Throws an exception unless 0 <= i < outDegree(id).
   */
  int outNeighbor(1:int id, 2:int i)
  /* Returns the ith in-neighbor of the given node.
       Throws an exception unless 0 <= i < inDegree(id).
   */
  int inNeighbor(1:int id, 2:int i)

  int maxNodeId()

  int nodeCount()

  long edgeCount()
}
