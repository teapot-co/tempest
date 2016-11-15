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
require 'tempest_db_constants'
require 'tempest_db_types'
require 'base_thrift_client'

# May also require the types and the constants for more complex services

module Teapot
  module TempestDB
    class TempestClient < Teapot::BaseThriftClient
      def init_executor(protocol)
        Teapot::TempestDB::TempestDBService::Client.new(protocol)
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

      def getMultiNodeAttribute(graphName, nodeIds, attributeName)
        id_to_json = get_executor.getMultiNodeAttributeAsJSON(graphName, nodeIds, attributeName)
        Hash[id_to_json.map{ |k,v| [k, TempestClient.jsonToValue(v)] }]
      end

      def getNodeAttribute(graphName, nodeId, attributeName)
        self.getMultiNodeAttribute(graphName, [nodeId], attributeName)[nodeId]
      end

      def kStepOutNeighborsFiltered(edgeType, sourceId, k, sqlClause: "", degreeFilter: {}, alternating: true)
        get_executor.kStepOutNeighborsFiltered(edgeType, sourceId, k, sqlClause, degreeFilter, alternating)
      end
      def kStepInNeighborsFiltered(edgeType, sourceId, k, sqlClause: "", degreeFilter: {}, alternating: true)
        get_executor.kStepInNeighborsFiltered(edgeType, sourceId, k, sqlClause, degreeFilter, alternating)
      end

      # Delegate other methods to the thrift generated method
      # Note: for autocomplete and documentation, we could improve usability by listing methods
      # explicitly (as we do for python), but that would require manual changes whenever the thrift
      # api changes, so for ruby we automatically delegate all other methods.
      def method_missing(m, *args)
        get_executor.send m, *args
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
