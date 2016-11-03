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

include "tempest.thrift"

namespace java co.teapot.tempest
namespace py tempest_db
namespace rb Teapot.TempestDB

exception UndefinedAttributeException {
  1:string message
}

exception UndefinedGraphException {
  1:string message
}

exception SQLException {
  1:string message
}

exception UnequalListSizeException {}

struct MonteCarloPageRankParams {
  1: required i32 numSteps; // The number of Monte Carlo steps
  2: required double resetProbability;
  // For performance, we may limit the number of neighbors we iterate over
  3: optional i32 maxIntermediateNodeDegree = 1000;
  4: optional i32 minReportedVisits;
  5: optional i32 maxResultCount; // If set, only the top maxResultCount nodes will be returned.

  // If true, treat graph as undirected; else use out-neighbors only
  6: optional bool undirectGraph = false;
}

enum DegreeFilterTypes { // Filters to apply to the results of a call that retrieves node neighborhoods
  INDEGREE_MIN = 0,
  INDEGREE_MAX = 1,
  OUTDEGREE_MIN = 2,
  OUTDEGREE_MAX = 3
}

typedef map<DegreeFilterTypes, i32> DegreeFilter

service TempestDBService extends tempest.TempestGraphService {
  # Returns a map from node id to attribute, with null attributes omitted.
  # Returns attributes in "JSON" format, meaning simply that strings are wrapped in double-quotes,
  # booleans are "true" or "false", and ints are returned as standard base-10 strings.
  # Our client wrappers should provide convenience methods to convert to natural client-side types,
  # and to allow a single nodeId argument.
  map<i64, string> getMultiNodeAttributeAsJSON(1:string nodeType, 2:list<i64> nodeIds, 3:string attributeName)
    throws (1: UndefinedGraphException error1, 2:UndefinedAttributeException error2)

  // TODO: re-enable setNodeAttribute once we enable mutable edges also
  //void setNodeAttribute(1:i64 nodeId, 2:string attributeName, 3:string attributeValue)

  list<i64> nodes(1:string nodeType, 2:string sqlClause)
    throws (1: UndefinedGraphException error1, 2: SQLException error2)

  list<i64> outNeighborsWithinKStepsFiltered(1:string edgeType, 2:i64 sourceId, 3:i32 k,
                                                  4:string sqlClause,
                                                  5:DegreeFilter filter)
    throws (1: UndefinedGraphException error1, 2: tempest.InvalidArgumentException error2,
            3: SQLException error3, 4: tempest.InvalidNodeIdException error4)

  list<i64> inNeighborsWithinKStepsFiltered(1:string edgeType, 2:i64 sourceId, 3:i32 k,
                                                 4:string sqlClause,
                                                 5:DegreeFilter filter)
    throws (1: UndefinedGraphException error1, 2: tempest.InvalidArgumentException error2,
            3: SQLException error3, 4: tempest.InvalidNodeIdException error4)


  map<i64, double> ppr(1:string edgeType, 2:list<i64> seeds,
                       3:MonteCarloPageRankParams pageRankParams)
    throws (1: UndefinedGraphException error1, 2: tempest.InvalidNodeIdException error2,
            3: tempest.InvalidArgumentException error3)

  // Add the given edges to the graph.
  // Since thrift doesn't have pairs, pass in parallel lists, which must have equal size or an
  // exception will be thrown.
  void addEdges(1:string edgeType, 2:list<i64> ids1,
                3:list<i64> ids2)
    throws (1: UndefinedGraphException error1, 2: UnequalListSizeException error2);
}
