#!/usr/bin/env ruby
#####################################
# tests for the tempest thrift server
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

client = get_tempest_client(server, port)

expect_equal(client.inDegree(1), 0, "Wrong indegree")
expect_equal(client.inDegree(2), 1, "Wrong indegree")

expect_equal(client.nodes("graph1", "username = 'bob'").sort, [2], "Wrong attribute values")
expect_equal(client.nodes("graph1", "login_count > 2").sort, [1, 3], "Wrong attribute values")
expect_equal(client.nodes("graph1", "login_count > 2 AND premium_subscriber = TRUE").sort, [3], "Wrong attribute values")

expect_equal(client.inNeighborsWithinKStepsFiltered("graph1", 2, 3, sqlClause: "login_count > 2"), [1], "Wrong in neighbors with attribute")
expect_equal(client.outNeighborsWithinKStepsFiltered("graph1", 1, 3, sqlClause: "login_count > 2").sort, [1, 3], "Wrong out neighbors with attribute")
expect_equal(client.outNeighborsWithinKStepsFiltered("graph1", 1, 3, sqlClause: "login_count > 2", degreeFilter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MIN => 2}), [], "Wrong filtering")
expect_equal(client.outNeighborsWithinKStepsFiltered("graph1", 1, 3, sqlClause: "login_count > 2", degreeFilter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MIN => 1}), [3], "Wrong filtering")
expect_equal(client.outNeighborsWithinKStepsFiltered("graph1", 1, 3, sqlClause: "login_count > 2", degreeFilter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MAX => 0}), [1], "Wrong filtering")
expect_equal(client.outNeighborsWithinKStepsFiltered("graph1", 1, 3, degreeFilter: {Teapot::TempestDB::DegreeFilterTypes::INDEGREE_MAX => 0, Teapot::TempestDB::DegreeFilterTypes::OUTDEGREE_MIN => 1}), [1], "Wrong AND of degree constraints")

expect_equal(client.getNodeAttribute("graph1", 1, "name"), 'Alice Johnson', "String attribute failed")
expect_equal(client.getNodeAttribute("graph1", 1, "login_count"), 5, "Int attribute failed")
expect_equal(client.getNodeAttribute("graph1", 1, "premium_subscriber"), false, "Boolean attribute failed")
id_to_login_count = client.getMultiNodeAttribute("graph1", [1, 3], "login_count")
expect_equal(id_to_login_count[1], 5, "multi-get failed")
expect_equal(id_to_login_count[3], 3, "multi-get failed")

# id 4 exists but has null name
expect_equal(client.getNodeAttribute("graph1", 4, "name"), nil, "null attribute failed")
expect_equal(client.getMultiNodeAttribute("graph1", [4], "name")[4], nil, "null attribute failed")
expect_equal(client.getNodeAttribute("graph1", 12345, "name"), nil, "null attribute failed")
expect_equal(client.getMultiNodeAttribute("graph1", [12345], "name")[4], nil, "null attribute failed")

# Test graph2 attributes
expect_equal(client.getNodeAttribute("graph2", 1, "username"), 'alice2', "String attribute failed")
expect_equal(client.getMultiNodeAttribute("graph2", [1, 4], "name")[4], nil, "null attribute failed")

expect_equal(client.nodes("graph2", "login_count2 = 32"), [3], "Wrong nodes")

expect_equal(
  client.outNeighborsWithinKStepsFiltered("graph2", 1, 2, sqlClause: "login_count2 < 30").sort,
  [2],
  "Wrong out neighbors with attribute")
