#!/usr/bin/env ruby
#####################################
# Tests for the tempest thrift server.
# This file is run by TempestDBServerClientSpec.
# For line-by-line debugging, launch TempestDBTestServer,
# run `irb -Iruby-package/lib`, and then paste the lines below into irb.
#####################################

# Requires the gem be installed, or ruby invoved with -Iruby-package/lib
require 'tempest_db'


server = ARGV[0] || "localhost"
port = (ARGV[1] || "10011").to_i

def expect_equal(actual_value, expected_value, message)
  raise "#{message} at #{caller(1)}: actual #{actual_value} != expected #{expected_value}" unless
    actual_value == expected_value
end

def expect_approx_equal(actual_value, expected_value, tol, message)
  raise "#{message} at #{caller(1)}: actual #{actual_value} != expected #{expected_value}" unless
    (actual_value - expected_value).abs < tol
end

def expect_exception(exception_type, &f)
  begin
    yield
    raise "Test failed: No exception at #{caller(1)}"
  rescue exception_type
  end
end

client = get_tempest_client(server, port)

alice = make_node("user", "alice")
bob = make_node("user", "bob")
carol = make_node("user", "carol")

expect_equal(client.in_degree("follows", alice), 0, "Wrong indegree")
expect_equal(client.in_degree("follows", bob), 2, "Wrong indegree")

expect_equal(client.nodes("user", "login_count = 5").sort, [alice], "Wrong attribute values")
expect_equal(client.unique_node("user", "login_count = 5"), alice, "unique_node failed")
expect_equal(client.nodes("user", "login_count > 2").sort, [alice, carol].sort, "Wrong attribute values")
expect_exception(RuntimeError) do
  client.unique_node("user", "login_count > 2")
end

expect_equal(client.nodes("user", "login_count > 2 AND premium_subscriber = TRUE").sort, [carol], "Wrong attribute values")


expect_equal(client.k_step_in_neighbors_filtered("follows", bob, 1, sql_clause: "login_count > 3"), [alice], "Wrong in neighbors with attribute")
expect_equal(client.k_step_in_neighbors_filtered("follows", bob, 1, sql_clause: "premium_subscriber = TRUE").sort, [carol], "Wrong in neighbors with attribute")

expect_equal(client.k_step_out_neighbors_filtered("follows", alice, 2, degree_filter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MIN => 1}), [carol], "Wrong filtering")
expect_equal(client.k_step_in_neighbors_filtered("follows", bob, 1, degree_filter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MAX => 0}), [alice], "Wrong filtering")

expect_equal(client.get_node_attribute(alice, "name"), 'Alice Johnson', "String attribute failed")
expect_equal(client.get_node_attribute(alice, "login_count"), 5, "Int attribute failed")
expect_equal(client.get_node_attribute(alice, "premium_subscriber"), false, "Boolean attribute failed")
id_to_login_count = client.get_multi_node_attribute([alice, carol], "login_count")
expect_equal(id_to_login_count[alice], 5, "multi-get failed")
expect_equal(id_to_login_count[carol], 3, "multi-get failed")

component1 = client.connected_component(alice, ['has_read'], 100)
expect_equal(component1.sort,
             [make_node("book", "101"), make_node("book", "102"), make_node("book", "103"),
             alice, bob, carol],
             "Wrong connected component")
component1_top3 = client.connected_component(alice, ['has_read'], 3)
expect_equal(component1_top3.sort,
             [make_node("book", "101"), make_node("book", "103"), alice],
             "Wrong size-bounded connected component")


# id 4 exists but has null name
nameless = make_node("user", "nameless")
invalid_node = make_node("user", "foo")
expect_equal(client.get_node_attribute(nameless, "name"), nil, "null attribute failed")
expect_equal(client.get_multi_node_attribute([nameless], "name")[4], nil, "null attribute failed")
expect_equal(client.get_node_attribute(invalid_node, "name"), nil, "null attribute failed")
expect_equal(client.get_multi_node_attribute([invalid_node], "name")[4], nil, "null attribute failed")

# Test 2nd node type (book) attributes
book101 = make_node("book", "101")
invalid_book = make_node("book", "foo")
expect_equal(client.get_node_attribute(book101, "title"), 'The Lord of the Rings', "String attribute failed")
expect_equal(client.get_multi_node_attribute([book101, invalid_book], "title")[invalid_book], nil, "null attribute failed")

expect_exception(Thrift::ApplicationException) do # TODO: This should be a declared exception, not ApplicationException
  client.get_node_attribute(alice, "title")
end

expect_equal(client.nodes("user", "id = 'alice'"), [alice], "Wrong nodes")
expect_equal(client.nodes("book", "id = 'foo'"), [], "Wrong book nodes")

expect_equal(
  client.k_step_out_neighbors_filtered("has_read", alice, 1, sql_clause: "title = 'The Lord of the Rings'").sort,
  [book101],
  "Wrong out neighbors with attribute")

puts "All Ruby client tests passed :)"
