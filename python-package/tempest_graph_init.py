# The __init__.py file for tempest_graph.  Contains the get_client method.

# Because thrift gen replaces __init__.py with its own, we store this here and copy it after running
# thrift gen.
# This is based on https://thrift.apache.org/tutorial/py

# Example:
# graph = get_client(host='localhost', port=10001)
# print('outneighbors(1): ' + str(graph.outNeighbors(1)))
# graph.close() # Close the TCP connection

__all__ = ['TempestService', 'get_client']


from tempest_graph import TempestService

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

def get_client(host='localhost', port=10001):
    # Make socket
    transport = TSocket.TSocket(host, port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = TempestService.Client(protocol)

    # Connect!
    transport.open()

    client.close = transport.close

    return client
