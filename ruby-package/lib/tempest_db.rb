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

      def initialize(server, port)
        @thrift_client = Teapot::TempestDB::TempestThriftClient.new(server, port)
      end

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

      def get_multi_node_attribute(nodes, attribute_name)
        node_to_json = @thrift_client.with_retries { |executor|
          executor.getMultiNodeAttributeAsJSON(nodes, attribute_name)
        }
        Hash[node_to_json.map{ |k,v| [k, TempestClient.jsonToValue(v)] }]
      end

      def get_node_attribute(node, attribute_name)
        self.get_multi_node_attribute([node], attribute_name)[node]
      end

      def k_step_out_neighbors_filtered(edge_type, source_node, k, sql_clause: "", degree_filter: {}, alternating: true)
        @thrift_client.with_retries { |executor|
          executor.kStepOutNeighborsFiltered(edge_type, source_node, k, sql_clause, degree_filter, alternating)
        }
      end

      def k_step_in_neighbors_filtered(edge_type, source_node, k, sql_clause: "", degree_filter: {}, alternating: true)
        @thrift_client.with_retries { |executor|
          executor.kStepInNeighborsFiltered(edge_type, source_node, k, sql_clause, degree_filter, alternating)
        }
      end

      # Convenience method to return the unique node node satisfying a condition
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
      def out_degree(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.outDegree(edge_type, node)
        }
      end

      def in_degree(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.inDegree(edge_type, node)
        }
      end

      def out_neighbors(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.outNeighbors(edge_type, node)
        }
      end

      def in_neighbors(edge_type, node)
        @thrift_client.with_retries { |executor|
          executor.inNeighbors(edge_type, node)
        }
      end

      def out_neighbor(edge_type, node, i)
        @thrift_client.with_retries { |executor|
          executor.outNeighbor(edge_type, node, i)
        }
      end

      def in_neighbor(edge_type, node, i)
        @thrift_client.with_retries { |executor|
          executor.inNeighbor(edge_type, node, i)
        }
      end

      def max_node_node(edge_type)
        @thrift_client.with_retries { |executor|
          executor.maxNodeId(edge_type)
        }
      end

      def node_count(edge_type)
        @thrift_client.with_retries { |executor|
          executor.nodeCount(edge_type)
        }
      end

      def edge_count(edge_type)
        @thrift_client.with_retries { |executor|
          executor.edgeCount(edge_type)
        }
      end

      def ppr_single_target(edge_type, seed_nodes, target_node, bi_ppr_params)
        @thrift_client.with_retries { |executor|
          executor.pprSingleTarget(edge_type, seed_nodes, target_node, bi_ppr_params)
        }
      end

      def ppr_undirected(edge_types, seed_nodes, mc_ppr_params)
        @thrift_client.with_retries { |executor|
          executor.ppr(edge_types, seed_nodes, mc_ppr_params)
        }
      end

      def connected_component(source, edge_types, max_size = (1 << 31) - 1)
        @thrift_client.with_retries { |executor|
          executor.connectedComponent(source, edge_types, max_size)
        }
      end

      def nodes(node_type, sql_clause)
        @thrift_client.with_retries { |executor|
          executor.nodes(node_type, sql_clause)
        }
      end

      def add_node(node)
        @thrift_client.with_retries { |executor|
          executor.addNode(node)
        }
      end

      def set_node_attribute(node, attribute_name, attribute_value)
        @thrift_client.with_retries { |executor|
          executor.setNodeAttribute(node, attribute_name, attribute_value)
        }
      end

      def add_edges(edge_type, source_nodes, target_nodes)
        @thrift_client.with_retries { |executor|
          executor.addEdges(edge_type, source_nodes, target_nodes)
        }
      end

      def add_edge(edge_type, source_node, target_node)
        add_edges(edge_type, [source_node], [target_node])
      end
    end
  end
end

def get_tempest_client(server = 'localhost', port = 10001)
  Teapot::TempestDB::TempestClient.new(server, port)
end

def get_empty_filter()
  Hash.new
end

def node_to_pair(node)
  [node.type, node.id]
end

def make_node(type, id)
  Teapot::TempestDB::Node.new({'type': type, 'id': id})
end
