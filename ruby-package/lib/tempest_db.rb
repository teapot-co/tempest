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

# This file defines a ruby TempestClient class for TempestDB.  The client wraps the methods
# provided by the thrift generated client, and also makes some methods easier to use.
# For example, it converts the types of attributes into the correct dynamic ruby type, and allows
# optional arguments (which thrift 0.9.3 doesn't support) for some methods.

$:.push(File.expand_path(File.join(File.dirname(__FILE__),"gen")))

require 'tempest_d_b_service'
require 'tempest_constants'
require 'tempest_types'
require 'base_thrift_client'

# May also require the types and the constants for more complex services

module Teapot
  module TempestDB
    class TempestThriftClient < Teapot::BaseThriftClient
      def init_executor(protocol)
        Teapot::TempestDB::TempestDBService::Client.new(protocol)
      end
    end

    class TempestClient
      @thrift_client = nil

      # @param [String] server
      # @param [Integer] port
      def initialize(server, port)
        @thrift_client = Teapot::TempestDB::TempestThriftClient.new(server, port)
      end

      # @param [String] json_attribute
      # @return [Object]
      def TempestClient.jsonToValue(json_attribute)
        if json_attribute[0] == '"'
          json_attribute[1...-1]
        elsif json_attribute == "true"
          true
        elsif json_attribute == "false"
          false
        elsif json_attribute == "null"
          nil
        else
          # If this isn't an int, the server made a mistake, and there isn't much the client can do.
          json_attribute.to_i
        end
      end

      # @param [Array<Teapot::TempestDB::Node>] nodes
      # @param [String] attribute_name
      # @return [Hash]
      def get_multi_node_attribute(nodes, attribute_name)
        node_to_json = @thrift_client.with_retries { |executor|
          executor.getMultiNodeAttributeAsJSON(nodes, attribute_name)
        }
        Hash[node_to_json.map{ |k,v| [k, TempestClient.jsonToValue(v)] }]
      end

      # @param [Teapot::TempestDB::Node] node
      # @param [String] attribute_name
      # @return [String]
      def get_node_attribute(node, attribute_name)
        self.get_multi_node_attribute([node], attribute_name)[node]
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] source_node
      # @param [Integer] k
      # @param [String] sql_clause
      # @param [Hash] degree_filter
      # @param [Boolean] alternating
      # @return [Teapot::TempestDB::Node]
      def k_step_out_neighbors_filtered(edge_type, source_node, k, sql_clause: "", degree_filter: {}, alternating: true)
        @thrift_client.with_retries { |executor|
          executor.kStepOutNeighborsFiltered(edge_type, source_node, k, sql_clause, degree_filter, alternating)
        }
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] source_node
      # @param [Integer] k
      # @param [String] sql_clause
      # @param [Hash] degree_filter
      # @param [Boolean] alternating
      # @return [Teapot::TempestDB::Node]
      def k_step_in_neighbors_filtered(edge_type, source_node, k, sql_clause: "", degree_filter: {}, alternating: true)
        @thrift_client.with_retries { |executor|
          executor.kStepInNeighborsFiltered(edge_type, source_node, k, sql_clause, degree_filter, alternating)
        }
      end

      # Convenience method to return the unique node node satisfying a condition
      # @param [String] node_type
      # @param [String] sql_clause
      # @return [Teapot::TempestDB::Node]
      def unique_node(node_type, sql_clause)
        matching_nodes = @thrift_client.with_retries { |executor|
          executor.nodes(node_type, sql_clause)
        }
        if matching_nodes.length != 1
          raise "Error: clause '#{sql_clause}' had #{matching_nodes.length} results"
        end
        matching_nodes[0]
      end


      # TODO: For methods that only need to be included (for irb auto-complete) and converted from camelCase to
      # snake_case, is there a simple way of automating this?
      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] node
      # @return [Integer]
      def out_degree(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.outDegree(edge_type, node)
        }
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] node
      # @return [Integer]
      def in_degree(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.inDegree(edge_type, node)
        }
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] node
      # @return [Array<Teapot::TempestDB::Node>]
      def out_neighbors(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.outNeighbors(edge_type, node)
        }
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] node
      # @return [Array<Teapot::TempestDB::Node>]
      def in_neighbors(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.inNeighbors(edge_type, node)
        }
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] node
      # @param [Integer] i
      # @return [Teapot::TempestDB::Node]
      def out_neighbor(edge_type, node, i)
        @thrift_client.with_retries { |executor|
          executor.outNeighbor(edge_type, node, i)
        }
      end

      # @param [String] edge_type
      # @param [Teapot::TempestDB::Node] node
      # @param [Integer] i
      # @return [Teapot::TempestDB::Node]
      def in_neighbor(edge_type, node, i)
        @thrift_client.with_retries { |executor|
          executor.inNeighbor(edge_type, node, i)
        }
      end

      # @param [String] edge_type
      # @return [Integer]
      def node_count(edge_type)
        @thrift_client.with_retries { |executor|
          executor.nodeCount(edge_type)
        }
      end

      # @param [String] edge_type
      # @return [Integer]
      def edge_count(edge_type)
        @thrift_client.with_retries { |executor|
          executor.edgeCount(edge_type)
        }
      end

      # @param [String] edge_type
      # @param [Array<Teapot::TempestDB::Node>] seed_nodes
      # @param [Teapot::TempestDB::Node] target_node
      # @param [Teapot::TempestDB::BidirectionalPPRParams] bi_ppr_params
      # @return [Double]
      def ppr_single_target(edge_type, seed_nodes, target_node, bi_ppr_params)
        @thrift_client.with_retries { |executor|
          executor.pprSingleTarget(edge_type, seed_nodes, target_node, bi_ppr_params)
        }
      end

      # @param [Teapot::TempestDB::Node] source
      # @param [Array<String>] edge_types
      # @param [Integer] max_size
      # @return [Array<Teapot::TempestDB::Node>]
      def connected_component(source, edge_types, max_size = (1 << 31) - 1)
        @thrift_client.with_retries { |executor|
          executor.connectedComponent(source, edge_types, max_size)
        }
      end

      # @param [String] node_type
      # @param [String] sql_clause
      # @return [Teapot::TempestDB::Node]
      def nodes(node_type, sql_clause)
        @thrift_client.with_retries { |executor|
          executor.nodes(node_type, sql_clause)
        }
      end

      # @param [String] edge_type
      # @param [Array<Teapot::TempestDB::Node>] source_nodes
      # @param [Array<Teapot::TempestDB::Node>] target_nodes
      def add_edges(edge_type, source_nodes, target_nodes)
        @thrift_client.with_retries { |executor|
          executor.addEdges(edge_type, source_nodes, target_nodes)
        }
      end
    end
  end
end

# @param [String] server
# @param [Integer] port
# @return [Teapot::TempestDB::TempestClient]
def get_tempest_client(server = 'localhost', port = 10001)
  Teapot::TempestDB::TempestClient.new(server, port)
end

# @return [Hash]
def get_empty_filter()
  Hash.new
end

# @param [Teapot::TempestDB::Node] node
# @return [Array]
def node_to_pair(node)
  [node.type, node.id]
end

# @param [String] type
# @param [String] id
# @return [Teapot::TempestDB::Node]
def make_node(type, id)
  Teapot::TempestDB::Node.new({'type' => type, 'id' => id})
end
