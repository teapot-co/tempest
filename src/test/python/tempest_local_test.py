#!/usr/bin/env python
# Test of python client
import tempest_graph
from tempest_graph.ttypes import *

def expect_equal(expected, actual):
    assert expected == actual, "expected " + str(expected) + " but actual " + str(actual)

def expect_approx_equal(expected, actual, tol):
    assert abs(expected - actual) < tol, \
        "expected " + str(expected) + " but actual " + str(actual) + " is not within " + str(tol)

def expect_exception(body, exception_type):
    try:
        body()
        assert False, "exception " + str(exception_type) + " not raised"
    except exception_type:
        pass

graph = tempest_graph.get_client(port=10012)

expect_equal(graph.outDegree(0), 2)
expect_equal(sorted(graph.outNeighbors(1)), [0, 2])

expect_exception(lambda: graph.outDegree(1234),
                 InvalidNodeIdException)
expect_exception(lambda: graph.outNeighbors(-1),
                 InvalidNodeIdException)

expect_exception(lambda: graph.outNeighbor(0, 2),
                 InvalidIndexException)

params = tempest_graph.BidirectionalPPRParams(relativeError=0.01, resetProbability=0.3)
estimate1_3 = graph.pprSingleTarget([1], 3, params)
expect_approx_equal(estimate1_3, 0.0512821, tol=0.01)

params = tempest_graph.BidirectionalPPRParams(relativeError=0.01,
                                              resetProbability=0.3,
                                              minProbability=-1.0)
expect_exception(lambda: graph.pprSingleTarget([1], 3, params),
                 InvalidArgumentException)
graph.close()
print "Python client tests passed :)"
