#!/usr/bin/env python
# Test of python client
import tempest_db
from tempest_db.tempest_graph.ttypes import InvalidNodeIdException
from tempest_db import twitter_2010_example

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

port = 10011
client = tempest_db.client(port=port)

expect_equal(client.out_degree(1), 1)
expect_equal(client.out_neighbors(1), [2])

# I haven't verified PPR_1[3] analytically, but 0.22 seems reasonable
expect_approx_equal(client.ppr("graph1", [1], num_steps=1000, reset_probability=0.3)[3], 0.22, 0.05)

expect_equal(client.node_attribute("graph1", 1, "name"), "Alice Johnson")
expect_equal(client.node_attribute("graph1", 1, "login_count"), 5)
expect_equal(client.node_attribute("graph1", 1, "premium_subscriber"), False)
expect_equal(client.node_attribute("graph1", 3, "premium_subscriber"), True)
id_to_login_count = client.multi_node_attribute("graph1", [1, 3], "login_count")
expect_equal(id_to_login_count[1], 5)
expect_equal(id_to_login_count[3], 3)

expect_equal(
        client.nodes("graph1", "username = 'bob'"),
        [2])

expect_equal(
        sorted(client.nodes("graph1", "login_count > 2")),
        [1, 3])

expect_equal(
  client.multi_hop_out_neighbors("graph1", 1, 3, "login_count > 2", min_in_degree=1),
  [3])
expect_equal(
  sorted(client.multi_hop_out_neighbors("graph1", 1, 3, filter="login_count > 2")),
  [1, 3])
expect_equal(
        sorted(client.multi_hop_in_neighbors("graph1", 3, 3, filter="login_count > 2")),
        [1, 3])
expect_equal(
  sorted(client.multi_hop_out_neighbors("graph1", 1, 3, min_in_degree=1)),
  [2, 3])
expect_equal(sorted(client.multi_hop_out_neighbors("graph1", 1, 3)), [1,2,3])

# id 4 exists but has null name
expect_equal(client.node_attribute("graph1", 4, "name"), None)
expect_equal(client.multi_node_attribute("graph1", [4], "name").get(4), None)

expect_equal(
        client.nodes("graph1", "username = 'non_existent_username'"),
        [])

expect_exception(lambda: client.nodes("graph1", "invalid_column = 'foo'"),
                 tempest_db.SQLException)
expect_exception(lambda: client.out_degree(1234),
                 InvalidNodeIdException)
expect_exception(lambda: client.multi_hop_out_neighbors("graph1", -1, 3),
                 InvalidNodeIdException)

expect_equal(sorted(twitter_2010_example.get_influencers("graph1", 'alice', client)),
             sorted(['bob', 'carol']))

#Try mutating the graph.  Keep these tests at the end to avoid cross-test interference.
client.add_edge("graph1", 1, 20)
expect_equal(client.out_degree(1), 2)
expect_equal(client.out_neighbors(1), [2, 20])
expect_equal(client.in_degree(20), 1)
expect_equal(client.in_neighbors(20), [1])

# TODO: Update test to use multiple graphs, once we store edges for multiple graphs.
client.add_edges("graph1", [1, 2], [3, 4])
expect_equal(client.out_neighbors(1), [2, 20, 3])
expect_equal(client.out_neighbors(2), [3, 4])
expect_equal(client.in_neighbors(3), [2, 1])


client.close()
