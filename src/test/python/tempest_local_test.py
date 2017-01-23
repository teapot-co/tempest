#!/usr/bin/env python
# Test of python client
# TODO: Fix scala test caller (mocking the DB?) so this can be run by sbt. For now, we run this
# manually inside docker.

import tempest_db
from tempest_db.tempest_graph.ttypes import InvalidNodeIdException, InvalidArgumentException
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

port = 10001
client = tempest_db.client(port=port)

expect_equal(client.out_degree("follows", 1), 1)
expect_equal(client.out_neighbors("follows", 1), [2])

# I haven't verified PPR_1[3] analytically, but 0.29 seems reasonable
expect_approx_equal(client.ppr("follows", [1], num_steps=10000, reset_probability=0.3)[3], 0.29, 0.01)

expect_equal(client.node_attribute("user", 1, "name"), "Alice Johnson")
expect_equal(client.node_attribute("user", 1, "login_count"), 5)
expect_equal(client.node_attribute("user", 1, "premium_subscriber"), False)
expect_equal(client.node_attribute("user", 3, "premium_subscriber"), True)
id_to_login_count = client.multi_node_attribute("user", [1, 3], "login_count")
expect_equal(id_to_login_count[1], 5)
expect_equal(id_to_login_count[3], 3)

expect_equal(
        client.nodes("user", "username = 'bob'"),
        [2])

expect_equal(
        sorted(client.nodes("user", "login_count > 2")),
        [1, 3])

expect_equal(
        client.multi_hop_out_neighbors("follows", 1, 2, alternating=False),
        [3])
expect_equal(
        client.multi_hop_out_neighbors("follows", 1, 2),
        [1, 3])

expect_equal(
  client.multi_hop_out_neighbors("follows", 1, 2, "login_count > 2", min_in_degree=1),
  [3])
expect_equal(
        client.multi_hop_out_neighbors("follows", 1, 2, "login_count > 2", min_in_degree=2),
        [])
expect_equal(
        sorted(client.multi_hop_in_neighbors("follows", 2, 1, filter="login_count > 2")),
        [1, 3])
expect_equal(
        sorted(client.multi_hop_in_neighbors("follows", 2, 1, filter="login_count = 5")),
        [1])
expect_equal(sorted(client.multi_hop_in_neighbors("follows", 2, 1)), [1, 3])

# id 4 exists but has null name
expect_equal(client.node_attribute("user", 4, "name"), None)
expect_equal(client.multi_node_attribute("user", [4], "name").get(4), None)

expect_equal(
        client.nodes("user", "username = 'non_existent_username'"),
        [])

expect_exception(lambda: client.nodes("user", "invalid_column = 'foo'"),
                 tempest_db.SQLException)
expect_exception(lambda: client.out_degree("follows", 1234),
                 InvalidNodeIdException)
expect_exception(lambda: client.multi_hop_out_neighbors("nonexistent_graph", -1, 3),
                 InvalidArgumentException)

# TODO: Update twitter_2010_example
#expect_equal(sorted(twitter_2010_example.get_influencers("follows", 'alice', client)),
#             sorted(['bob', 'carol']))

#Try mutating the graph.  Keep these tests at the end to avoid cross-test interference.
client.add_edge("follows", 1, 20)
expect_equal(client.out_degree("follows", 1), 2)
expect_equal(client.out_neighbors("follows", 1), [2, 20])
expect_equal(client.in_degree("follows", 20), 1)
expect_equal(client.in_neighbors("follows", 20), [1])

client.add_edges("has_read", [1, 2], [100, 200])
expect_equal(client.out_neighbors("has_read", 1), [1, 3, 100])
expect_equal(client.out_neighbors("has_read", 2), [1, 2, 3, 200])
expect_equal(client.in_neighbors("has_read", 200), [2])


client.close()
