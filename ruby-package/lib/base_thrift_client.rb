require 'thrift'

module Teapot
    class BaseThriftClient

      @server = nil
      @port = nil
      @executor = nil
      @transport = nil

      def initialize(server, port)
        @server = server
        @port = port
        @executor = init_executor(init_protocol())
      end

      def init_protocol
        @transport = Thrift::BufferedTransport.new(Thrift::Socket.new(@server, @port))
        @transport.open
        Thrift::BinaryProtocol.new(@transport)
      end

      def init_executor(protocol)
        raise NotImplementedError, 'Undefined method in BaseThriftClient. init_executor() method should be implemented by the subclass.'
      end

      def close
        @transport.close
      end

      def get_executor
        @executor
      end

    end
end

