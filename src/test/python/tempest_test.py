#!/usr/bin/env python
# Test of Tempest server from python
# This file is run by TempestDBServerClientSpec.
# For line-by-line debugging, launch TempestDBTestServer,
# run `export PYTHONPATH='python-package'; ipython`, and then paste the lines below into ipython.

import tempest_db
from tempest_db import Node

def expect_equal(actual, expected):
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

alice = Node("user", "alice")
bob = Node("user", "bob")
carol = Node("user", "carol")

expect_equal(client.out_degree("follows", alice), 1)
expect_equal(client.out_neighbors("follows", alice), [bob])

# I haven't verified PPR_alice[bob] analytically, but 0.41 seems reasonable
expect_approx_equal(client.ppr_undirected(["follows"], [alice], num_steps=10000, reset_probability=0.3)[bob], 0.41, 0.02)


ppr_alice_carol = client.ppr_single_target("follows", [alice], bob, relative_error=0.01, reset_probability=0.3)
# I haven't verified PPR_alice[bob] analytically, but 0.41 seems reasonable
expect_approx_equal(ppr_alice_carol, 0.41, tol=0.01)

expect_equal(client.node_attribute(alice, "name"), "Alice Johnson")
expect_equal(client.node_attribute(alice, "login_count"), 5)
expect_equal(client.node_attribute(alice, "premium_subscriber"), False)
expect_equal(client.node_attribute(carol, "premium_subscriber"), True)
id_to_login_count = client.multi_node_attribute([alice, carol], "login_count")
expect_equal(id_to_login_count[alice], 5)
expect_equal(id_to_login_count[carol], 3)

expect_equal(
        client.nodes("user", "name = 'Bob Smith, Jr.'"),
        [bob])

expect_equal(
        set(client.nodes("user", "login_count > 2")),
        set([alice, carol]))

expect_equal(
        client.multi_hop_out_neighbors("follows", alice, 2, alternating=False),
        [carol])
expect_equal(
        set(client.multi_hop_out_neighbors("follows", alice, 2)),
        set([alice, carol]))

expect_equal(
  client.multi_hop_out_neighbors("follows", alice, 2, "login_count > 2", min_in_degree=1),
  [carol])
expect_equal(
        client.multi_hop_out_neighbors("follows", alice, 2, "login_count > 2", min_in_degree=2),
        [])
expect_equal(
        set(client.multi_hop_in_neighbors("follows", bob, 1, filter="login_count > 2")),
        set([alice, carol]))
expect_equal(
        client.multi_hop_in_neighbors("follows", bob, 1, filter="login_count = 5"),
        [alice])
expect_equal(set(client.multi_hop_in_neighbors("follows", bob, 1)), set([alice, carol]))

# id 4 exists but has null name
nameless = Node("user", "nameless")
expect_equal(client.node_attribute(nameless, "name"), None)
expect_equal(client.multi_node_attribute([nameless], "name").get(4), None)

expect_equal(
        client.nodes("user", "name = 'non_existent_name'"),
        [])

expect_exception(lambda: client.nodes("user", "invalid_column = 'foo'"),
                 tempest_db.SQLException)
expect_exception(lambda: client.out_degree("follows", Node("user", "foo")),
                 tempest_db.InvalidNodeIdException)
expect_exception(lambda: client.multi_hop_out_neighbors("nonexistent_graph", alice, 3),
                 tempest_db.UndefinedGraphException)
expect_exception(lambda: client.out_neighbor("follows", alice, 2),
                 tempest_db.InvalidIndexException)

# TODO: Update twitter_2010_example
#expect_equal(sorted(twitter_2010_example.get_influencers("follows", 'alice', client)),
#             sorted(['bob', 'carol']))

test_mutation = False
# After running mutation tests, to avoid interference with tests above, recreate the test graph in docker by running the
# example node/edge creation commands from the readme.
# We've disabled these until we find a way to keep the mutated graph from changing results of future tests.
if test_mutation:
    daniel = Node("user", "daniel")
    client.add_node(daniel)
    client.set_node_attribute(daniel, "name", "Daniel Smith")
    client.add_edge("follows", alice, daniel)
    expect_equal(client.out_degree("follows", alice), 2)

    # TODO: Make this test work
    # client.set_node_attribute(daniel, "login_count", 42)

    expect_equal(client.out_neighbors("follows", alice), [bob, daniel])
    expect_equal(client.in_degree("follows", daniel), 1)
    expect_equal(client.in_neighbors("follows", daniel), [alice])
    expect_equal(client.out_neighbors("has_read", daniel), [])

    book101 = Node("book", "101")
    book102 = Node("book", "102")
    book103 = Node("book", "103")
    client.add_edges("has_read", [alice, daniel], [book102, book103])
    expect_equal(set(client.out_neighbors("has_read", alice)), set([book101, book102, book103]))
    expect_equal(client.out_neighbors("has_read", daniel), [book103])

client.close()
print "Python client tests passed :)"
