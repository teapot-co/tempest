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

exception InvalidNodeIdException {
  1:string message
}

exception InvalidIndexException {
  1:string message
}

exception InvalidArgumentException {
  1:string message
}

struct BidirectionalPPRParams {
  1: required double relativeError;
  2: required double resetProbability;
  // The minimum PPR value we detect (smaller values are set to 0.0).
  // Currently defaults to 0.25 / maxNodeId
  3: optional double minProbability;
}

service TempestService {
  int outDegree(1:int id) throws (1:InvalidNodeIdException e)
  int inDegree(1:int id) throws (1:InvalidNodeIdException e)

  list<int> outNeighbors(1:int id) throws (1:InvalidNodeIdException e)
  list<int> inNeighbors(1:int id) throws (1:InvalidNodeIdException e)

  /* Returns the ith out-neighbor of the given node.
     Throws an exception unless 0 <= i < outDegree(id).
   */
  int outNeighbor(1:int id, 2:int i) throws (1:InvalidNodeIdException ex1, 2:InvalidIndexException ex2)
  /* Returns the ith in-neighbor of the given node.
       Throws an exception unless 0 <= i < inDegree(id).
   */
  int inNeighbor(1:int id, 2:int i) throws (1:InvalidNodeIdException ex1, 2:InvalidIndexException ex2)

  int maxNodeId()

  int nodeCount()

  long edgeCount()

  // TODO: Basic Monte Carlo PPR

  // Estimates the PPR of the given target personId personalized to the uniform distribution over
  // the seed personIds.  If the PPR is significant (currently meaning >= 0.25 / maxNodeId), the
  // estimate will have relative error less than the given relative error (in expectation).
  // If the PPR is not significant, returns 0.0.
  double pprSingleTarget(1:list<int> seedPersonIds,
                         2:int targetPersonId,
                         3:BidirectionalPPRParams biPPRParams)
    throws (1:InvalidNodeIdException ex1, 2:InvalidArgumentException ex2)
}
