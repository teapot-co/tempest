# Copyright 2016 Teapot, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
# file except in compliance with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

# The __init__.py file for tempest_graph.  Contains the get_client method.

# Because thrift gen replaces __init__.py with its own, we store this here and copy it after running
# thrift gen.
# This is based on https://thrift.apache.org/tutorial/py

# Example:
# graph = get_client(host='localhost', port=10001)
# print('outneighbors(1): ' + str(graph.outNeighbors(1)))
# graph.close() # Close the TCP connection

__all__ = [
    'TempestDBService',
    'TempestClient',
    'client',
    'UndefinedAttributeException',
    'SQLException'
    'tempest_graph',
    'twitter_2010_example']

import ttypes
import tempest_graph
import tempest_graph.ttypes
from tempest_db import TempestDBService
MonteCarloPageRankParams = ttypes.MonteCarloPageRankParams
BidirectionalPPRParams = tempest_graph.ttypes.BidirectionalPPRParams
DegreeFilterTypes = ttypes.DegreeFilterTypes
UndefinedAttributeException = ttypes.UndefinedAttributeException
SQLException = ttypes.SQLException

import twitter_2010_example

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

import sys

def get_thrift_client(host, port):
    # Make socket
    transport = TSocket.TSocket(host, port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = TempestDBService.Client(protocol)

    # Connect!
    transport.open()

    client.close = transport.close # Useful for closing transport later

    return client

class TempestClient:
    """Client class for querying TempestDB."""

    def __init__(self, host='localhost', port=10001):
        """ Create a new client to a Tempest server on the given host and port."""
        self.__host = host
        self.__port = port
        self.__thrift_client = get_thrift_client(host, port)
        self.__max_retries = 3

    def __with_retries(self, f):
        """ Call the given function (which typically contains a reference to self.__thrift_client), retrying
        on error, and return whatever it returns.
        """
        retry_count = 0
        while retry_count < self.__max_retries:
            try:
                return f()
            except (TTransport.TTransportException, IOError):
                sys.stderr.write("(Tempest client reconnecting to server...)\n")
                # Note that get_thrift_client might throw an exception if the server still isn't
                # available, which we just allow.
                self.__thrift_client = get_thrift_client(self.__host, self.__port)
                retry_count += 1
            except KeyboardInterrupt:
                print 'Interrupted'
                # Try to close the old client, ignoring any failure.
                try: self.__thrift_client.close()
                except TTransport.TTransportException: pass
                self.__thrift_client = get_thrift_client(self.__host, self.__port)
                return None

    def node_count(self):
        """ Return the number of nodes."""
        return self.__with_retries(lambda: self.__thrift_client.nodeCount())

    def edge_count(self, edge_type):
        """ Return the number of edges."""
        return self.__with_retries(lambda: self.__thrift_client.edgeCount(edge_type))

    def max_node_id(self):
        """ Return the largest node id."""
        return self.__with_retries(lambda: self.__thrift_client.maxNodeId())

    def out_degree(self, edge_type, node_id):
        """ Return the out-degree of the given node_id."""
        return self.__with_retries(lambda: self.__thrift_client.outDegree(edge_type, node_id))

    def out_neighbors(self, edge_type, node_id):
        """ Return the out-neighbors of the given node_id."""
        return self.__with_retries(lambda: self.__thrift_client.outNeighbors(edge_type, node_id))

    def out_neighbor(self, edge_type, node_id, i):
        """ Return the ith out-neighbor of the given node_id."""
        return self.__with_retries(lambda: self.__thrift_client.outNeighbor(edge_type, node_id, i))

    def in_degree(self, edge_type, node_id):
        """ Return the in-degree of the given node_id."""
        return self.__with_retries(lambda: self.__thrift_client.inDegree(edge_type, node_id))

    def in_neighbors(self, edge_type, node_id):
        """ Return the in-neighbors of the given node_id."""
        return self.__with_retries(lambda: self.__thrift_client.inNeighbors(edge_type, node_id))

    def in_neighbor(self, edge_type, node_id, i):
        """ Return the ith in-neighbor of the given node_id."""
        return self.__with_retries(lambda: self.__thrift_client.inNeighbor(edge_type, node_id, i))

    # Private for now
    def _ppr_single_target(self, edge_type, seeds, target, relative_error=0.1, reset_probability=0.3,
                          min_probability=None):
        """Return the Personalized PageRank of the target node id personalized to the seed node ids.
        If the PPR is greater than min_probability (default 0.25 / node_count),
        the estimate will have relative error less than the given relative error bound (on average).
        If the PPR is is less than min_probability, return 0.0."""
        params = BidirectionalPPRParams(relativeError=relative_error,
                                        resetProbability=reset_probability)
        if min_probability:
            params.minProbability = min_probability
        return self.__with_retries(lambda: self.__thrift_client.pprSingleTarget(edge_type, seeds, target, params))


    # TempestDB methods
    def ppr(self, edge_type, seeds, seed_node_type, target_node_type, num_steps=100000, reset_probability=0.3, alternating = True, max_results = None):
        """Return a dictionary from node id to Personalized PageRank, personalized to the
        seed node ids.  Compute this by doing the given number of random
        walks."""
        params = MonteCarloPageRankParams(numSteps=num_steps, resetProbability=reset_probability)
        params.alternatingWalk = alternating
        if max_results:
            params.maxResultCount = max_results
        return self.__with_retries(lambda: self.__thrift_client.ppr(edge_type, seeds, seed_node_type, target_node_type, params))

    def nodes(self, graph_name, filter):
        """Return all node ids satisfying the given SQL-like filter clause"""
        return self.__with_retries(lambda: self.__thrift_client.nodes(graph_name, filter))

    def multi_hop_out_neighbors(self, edge_type, source_id, max_hops, filter="",
                                max_out_degree=None, max_in_degree=None,
                                min_out_degree=None, min_in_degree=None,
                                alternating=True):
        """ Return ids of all nodes which are max_hops out-neighbor steps from the source id,
        optionally filtered by the given SQL filter max/min degree bounds."""
        degreeFilter={}
        if max_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MAX] = max_out_degree
        if min_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MIN] = min_out_degree
        if max_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MAX] = max_in_degree
        if min_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MIN] = min_in_degree
        return self.__with_retries(lambda:
            self.__thrift_client.kStepOutNeighborsFiltered(edge_type, source_id, max_hops, filter, degreeFilter, alternating))

    def multi_hop_in_neighbors(self, edge_type, source_id, max_hops, filter="",
                               max_out_degree=None, max_in_degree=None,
                               min_out_degree=None, min_in_degree=None,
                               alternating=True):
        """ Return ids of all nodes which are max_hops in-neighbor steps from the source id,
        optionally filtered by the given SQL filter max/min degree bounds."""
        degreeFilter={}
        if max_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MAX] = max_out_degree
        if min_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MIN] = min_out_degree
        if max_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MAX] = max_in_degree
        if min_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MIN] = min_in_degree
        return self.__with_retries(lambda:
            self.__thrift_client.kStepInNeighborsFiltered(edge_type, source_id, max_hops, filter, degreeFilter, alternating))

    def node_attribute(self, graph_name, node_id, attribute_name):
        # get will return None if attribute_name isn't found (e.g. if it was null in the database)
        return self.multi_node_attribute(graph_name, [node_id], attribute_name).get(node_id)

    def multi_node_attribute(self, graph_name, node_ids, attribute_name):
        """ Return a dictionary mapping node id to attribute value for the given node ids and attribute name.
        Omits node ids which have a null attribute value."""
        return  self.__with_retries(lambda:
            {k: jsonToValue(v) for k, v in
             self.__thrift_client.getMultiNodeAttributeAsJSON(graph_name, node_ids, attribute_name).items()})

    def close(self):
        """Close the TCP connection to the server."""
        self.__thrift_client.close()

    def add_edges(self, edge_type,  ids1, ids2):
        """ Adds edges from corresponding items in the given parallel lists to the graph. """
        self.__thrift_client.addEdges(edge_type, ids1, ids2)

    def add_edge(self, edge_type,  id1, id2):
        """ Adds the given edge to the graph. """
        self.add_edges(edge_type, [id1], [id2])


def jsonToValue(json_attribute):
    if json_attribute[0] == '"':
        return json_attribute[1:-1]
    elif json_attribute == "true":
        return True
    elif json_attribute == "false":
        return False
    elif json_attribute == "null":
        return None
    else:
        # If this isn't an int, the server made a mistake, and there isn't much the client can do.
        return int(json_attribute)

def client(host='localhost', port=10001):
    """ Create a new client to a Tempest server on the given host and port."""
    return TempestClient(host, port)
