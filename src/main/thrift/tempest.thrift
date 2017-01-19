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

namespace java co.teapot.tempest
namespace py tempest_graph

typedef i32 int
typedef i64 long

struct Node {
  1: required string type;
  2: required string id;
}

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

service TempestGraphService {
  int outDegree(1:string edgeType, 2:Node node) throws (1:InvalidNodeIdException ex1, 2:InvalidArgumentException ex2)
  int inDegree(1:string edgeType, 2:Node node) throws (1:InvalidNodeIdException ex1, 2:InvalidArgumentException ex2)

  list<Node> outNeighbors(1:string edgeType, 2:Node node) throws (1:InvalidNodeIdException ex1, 2:InvalidArgumentException ex2)
  list<Node> inNeighbors(1:string edgeType, 2:Node node) throws (1:InvalidNodeIdException ex1, 2:InvalidArgumentException ex2)

  /* Returns the ith out-neighbor of the given node.
     Throws an exception unless 0 <= i < outDegree(node).
   */
  Node outNeighbor(1:string edgeType, 2:Node node, 3:int i) throws (1:InvalidNodeIdException ex1, 2:InvalidIndexException ex2, 3:InvalidArgumentException ex3)

  /* Returns the ith in-neighbor of the given node.
       Throws an exception unless 0 <= i < inDegree(node).
   */
  Node inNeighbor(1:string edgeType, 2:Node node, 3:int i) throws (1:InvalidNodeIdException ex1, 2:InvalidIndexException ex2, 3:InvalidArgumentException ex3)

  int maxNodeId(1:string edgeType) throws (1:InvalidArgumentException ex)

  int nodeCount(1:string edgeType) throws (1:InvalidArgumentException ex)

  long edgeCount(1:string edgeType) throws (1:InvalidArgumentException ex)


  // Estimates the PPR of the given target node personalized to the uniform distribution over
  // the seed nodes.  If the PPR is significant (currently meaning >= 0.25 / maxNodeId), the
  // estimate will have relative error less than the given relative error (in expectation).
  // If the PPR is not significant, returns 0.0.
  double pprSingleTarget(1:string edgeType,
                         2:list<Node> seedNodes,
                         3:Node targetNode,
                         4:BidirectionalPPRParams biPPRParams)
    throws (1:InvalidNodeIdException ex1, 2:InvalidArgumentException ex2)
}

