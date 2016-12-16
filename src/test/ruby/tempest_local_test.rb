#!/usr/bin/env ruby
#####################################
# tests for the tempest thrift server
#####################################

# Requires the gem be installed, or ruby invoved with -Iruby-package/lib
require 'tempest_db'


server = ARGV[0] || "localhost"
port = (ARGV[1] || "10001").to_i

def expect_equal(actual_value, expected_value, message)
  raise "#{message} at #{caller(1)}: actual #{actual_value} != expected #{expected_value}" unless
    actual_value == expected_value
end

def expect_approx_equal(actual_value, expected_value, tol, message)
  raise "#{message} at #{caller(1)}: actual #{actual_value} != expected #{expected_value}" unless
    (actual_value - expected_value).abs < tol
end

def expect_exception(&f)
  begin
    f.call
    raise "Test failed: No exception at #{caller(1)}"
  rescue Exception
  end
end

client = get_tempest_client(server, port)

expect_equal(client.in_degree("follows", 1), 0, "Wrong indegree")
expect_equal(client.in_degree("follows", 2), 2, "Wrong indegree")

expect_equal(client.nodes("user", "username = 'bob'").sort, [2], "Wrong attribute values")
expect_equal(client.unique_node("user", "username = 'bob'"), 2, "unique_node failed")
expect_equal(client.nodes("user", "login_count > 2").sort, [1, 3], "Wrong attribute values")
expect_exception do
  client.unique_node("user", "login_count > 2")
end

expect_equal(client.nodes("user", "login_count > 2 AND premium_subscriber = TRUE").sort, [3], "Wrong attribute values")

expect_equal(client.k_step_in_neighbors_filtered("follows", 2, 1, sql_clause: "login_count > 3"), [1], "Wrong in neighbors with attribute")
expect_equal(client.k_step_in_neighbors_filtered("follows", 2, 1, sql_clause: "premium_subscriber = TRUE").sort, [3], "Wrong in neighbors with attribute")

expect_equal(client.k_step_out_neighbors_filtered("follows", 1, 2, degree_filter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MIN => 1}), [3], "Wrong filtering")
expect_equal(client.k_step_in_neighbors_filtered("follows", 2, 1, degree_filter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MAX => 0}), [1], "Wrong filtering")

expect_equal(client.get_node_attribute("user", 1, "name"), 'Alice Johnson', "String attribute failed")
expect_equal(client.get_node_attribute("user", 1, "login_count"), 5, "Int attribute failed")
expect_equal(client.get_node_attribute("user", 1, "premium_subscriber"), false, "Boolean attribute failed")
id_to_login_count = client.get_multi_node_attribute("user", [1, 3], "login_count")
expect_equal(id_to_login_count[1], 5, "multi-get failed")
expect_equal(id_to_login_count[3], 3, "multi-get failed")

# id 4 exists but has null name
expect_equal(client.get_node_attribute("user", 4, "name"), nil, "null attribute failed")
expect_equal(client.get_multi_node_attribute("user", [4], "name")[4], nil, "null attribute failed")
expect_equal(client.get_node_attribute("user", 12345, "name"), nil, "null attribute failed")
expect_equal(client.get_multi_node_attribute("user", [12345], "name")[4], nil, "null attribute failed")

# Test 2nd node type (book) attributes
expect_equal(client.get_node_attribute("book", 1, "title"), 'The Lord of the Rings', "String attribute failed")
expect_equal(client.get_multi_node_attribute("book", [1, 4], "title")[4], nil, "null attribute failed")

expect_equal(client.nodes("user", "id = 4"), [4], "Wrong nodes")
expect_equal(client.nodes("book", "id = 4"), [], "Wrong book nodes")

expect_equal(
  client.k_step_out_neighbors_filtered("has_read", 1, 1, sql_clause: "title = 'The Lord of the Rings'").sort,
  [101],
  "Wrong out neighbors with attribute")
