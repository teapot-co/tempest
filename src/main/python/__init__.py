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

# The __init__.py file for tempest_db.  Contains the get_client method.

# Because thrift gen replaces __init__.py with its own, we store this here and copy it after running
# thrift gen.
# This is based on https://thrift.apache.org/tutorial/py

# Example:
#   graph = get_client(host='localhost', port=10001)
#   print('alice's outneighbors: ' + str(graph.out_neighbors(Node("user", "alice")))
#   graph.close() # Close the TCP connection
# See tests in src/test/python for more examples.

__all__ = [
    'TempestDBService',
    'TempestClient',
    'client',
    'Node',
    'BidirectionalPPRParams',

    'InvalidArgumentException',
    'SQLException',
    'UndefinedGraphException',
    'InvalidNodeIdException',
    'InvalidIndexException',

    'twitter_2010_example']

import ttypes
from tempest_db import TempestDBService
MonteCarloPageRankParams = ttypes.MonteCarloPageRankParams
BidirectionalPPRParams = ttypes.BidirectionalPPRParams
DegreeFilterTypes = ttypes.DegreeFilterTypes
InvalidArgumentException = ttypes.InvalidArgumentException
SQLException = ttypes.SQLException
UndefinedGraphException = ttypes.UndefinedGraphException
InvalidNodeIdException = ttypes.InvalidNodeIdException
InvalidIndexException = ttypes.InvalidIndexException
BidirectionalPPRParams = ttypes.BidirectionalPPRParams
Node = ttypes.Node

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

    def out_degree(self, edge_type, node):
        """ Return the out-degree of the given node."""
        return self.__with_retries(lambda: self.__thrift_client.outDegree(edge_type, node))

    def out_neighbors(self, edge_type, node):
        """ Return the out-neighbors of the given node."""
        return self.__with_retries(lambda: self.__thrift_client.outNeighbors(edge_type, node))

    def out_neighbor(self, edge_type, node, i):
        """ Return the ith out-neighbor of the given node."""
        return self.__with_retries(lambda: self.__thrift_client.outNeighbor(edge_type, node, i))

    def in_degree(self, edge_type, node):
        """ Return the in-degree of the given node."""
        return self.__with_retries(lambda: self.__thrift_client.inDegree(edge_type, node))

    def in_neighbors(self, edge_type, node):
        """ Return the in-neighbors of the given node."""
        return self.__with_retries(lambda: self.__thrift_client.inNeighbors(edge_type, node))

    def in_neighbor(self, edge_type, node, i):
        """ Return the ith in-neighbor of the given node."""
        return self.__with_retries(lambda: self.__thrift_client.inNeighbor(edge_type, node, i))

    def ppr_single_target(self, edge_type, seeds, target, relative_error=0.1, reset_probability=0.3,
                          min_probability=None):
        """Return the Personalized PageRank of the target node personalized to the seed nodes.
        If the PPR is greater than min_probability (default 0.25 / node_count),
        the estimate will have relative error less than the given relative error bound (on average).
        If the PPR is is less than min_probability, return 0.0."""
        params = BidirectionalPPRParams(relativeError=relative_error,
                                        resetProbability=reset_probability)
        if min_probability:
            params.minProbability = min_probability
        return self.__with_retries(lambda: self.__thrift_client.pprSingleTarget(edge_type, seeds, target, params))


    # TempestDB methods
    def ppr_undirected(self, edge_types, seeds, num_steps=100000, reset_probability=0.3, max_results = None):
        """Return a dictionary from node to Personalized PageRank, personalized to the
        seed node ids.  Compute this by doing the given number of random
-        walks. Seed_node_type and target_node_type are the node types of the seeds and targets, and they must be one of
        the node types related by the given edge_type.
        Parameter alternating indicates the walk alternates between following out-edges and following in-edges.
        For example, if alternating=False, and the graph is Twitter, the walk might go from a user Alice, to a user she
        follows like Obama, to a user he follows like Biden.  Wheras if alternating=True, the walk might go from
        Alice to Obama to some other person who follows Obama, say Bob.  For bipartite graphs it's mandatory that alternating=True.
        See MonteCarloPPR.scala for a more detailed explanation of PPR parameters."""
        params = MonteCarloPageRankParams(numSteps=num_steps, resetProbability=reset_probability)
        if max_results:
            params.maxResultCount = max_results
        return self.__with_retries(lambda: self.__thrift_client.pprUndirected(edge_types, seeds, params))

    def connected_component(self, source, edge_types, max_size = (1 << 31) - 1):
        return self.__with_retries(lambda: self.__thrift_client.connectedComponent(source, edge_types, max_size))

    def nodes(self, graph_name, filter):
        """Return all nodes satisfying the given SQL-like filter clause"""
        return self.__with_retries(lambda: self.__thrift_client.nodes(graph_name, filter))

    def multi_hop_out_neighbors(self, edge_type, source_node, max_hops, filter="",
                                max_out_degree=None, max_in_degree=None,
                                min_out_degree=None, min_in_degree=None,
                                alternating=True):
        """ Return all nodes which are max_hops out-neighbor steps from the source node,
        optionally filtered by the given SQL filter max/min degree bounds."""
        degreeFilter={}
        if max_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MAX] = max_out_degree
        if min_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MIN] = min_out_degree
        if max_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MAX] = max_in_degree
        if min_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MIN] = min_in_degree
        return self.__with_retries(lambda:
            self.__thrift_client.kStepOutNeighborsFiltered(edge_type, source_node, max_hops, filter, degreeFilter, alternating))

    def multi_hop_in_neighbors(self, edge_type, source_node, max_hops, filter="",
                               max_out_degree=None, max_in_degree=None,
                               min_out_degree=None, min_in_degree=None,
                               alternating=True):
        """ Return all nodes which are max_hops in-neighbor steps from the source node,
        optionally filtered by the given SQL filter max/min degree bounds."""
        degreeFilter={}
        if max_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MAX] = max_out_degree
        if min_out_degree: degreeFilter[DegreeFilterTypes.OUTDEGREE_MIN] = min_out_degree
        if max_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MAX] = max_in_degree
        if min_in_degree: degreeFilter[DegreeFilterTypes.INDEGREE_MIN] = min_in_degree
        return self.__with_retries(lambda:
            self.__thrift_client.kStepInNeighborsFiltered(edge_type, source_node, max_hops, filter, degreeFilter, alternating))

    def node_attribute(self, node, attribute_name):
        # get will return None if attribute_name isn't found (e.g. if it was null in the database)
        return self.multi_node_attribute([node], attribute_name).get(node)

    def multi_node_attribute(self, nodes, attribute_name):
        """ Return a dictionary mapping node to attribute value for the given nodes and attribute name.
        Omits node ids which have a null attribute value."""
        return  self.__with_retries(lambda:
            {k: jsonToValue(v) for k, v in
             self.__thrift_client.getMultiNodeAttributeAsJSON(nodes, attribute_name).items()})

    def close(self):
        """Close the TCP connection to the server."""
        self.__thrift_client.close()

    def add_node(self, node):
        """ Create the given node, so edges and attributes can be set on it."""
        self.__thrift_client.addNode(node)

    def set_node_attribute(self, node, attribute_name, attribute_value):
        """Set the given attribute on the given node, which must have been added previously."""
        self.__thrift_client.setNodeAttribute(node, attribute_name, attribute_value)

    def add_edges(self, edge_type,  nodes1, nodes2):
        """ Adds edges from corresponding items in the given parallel lists to the graph. """
        self.__thrift_client.addEdges(edge_type, nodes1, nodes2)

    def add_edge(self, edge_type,  node1, node2):
        """ Adds the given edge to the graph. """
        self.add_edges(edge_type, [node1], [node2])


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
