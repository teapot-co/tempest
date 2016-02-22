# Tempest Graph Library
Tempest is a graph library for efficiently handling large graphs. For example, it can store the 
Twitter-2010 graph of 1.5 billion edges with bi-directional edge access using 12GB of RAM.  Once 
a graph is has been converted to the Tempest file format, Tempest reads it directly from the 
file, so there is no parsing required and no object or garbage collector overhead.  Once the 
graph file is cached in RAM by the OS, programs using tempest can read the graph instantly rather
 than load the graph as other libraries would.

<!--To handle large graphs efficiently, this repository provides a MemoryMappedDirectedGraph class,
which stores graphs in an efficient binary format that requires only 8 bytes per node and 8 bytes
 per edge.  The data is stored in a memory mapped file, which has two benefits relative to 
 storing it in arrays:
 1. There is no object overhead per node and no garbage collector overhead.
 2. Rather than parse a text or binary file, the load operation simply memory maps a file.  If 
    the graph file is in the OS cache, the graph loads almost instantly.-->

For large graphs, you will want to first convert your graph to the Tempest binary format.  The 
input to our converter is a simple text format where each line has the form "id1 id2" to 
indicate an edge from id1 to id2, where id1 and id2 are Ints.  To convert the test graph, for 
example, run:
```
git clone https://github.com/teapot/tempest.git
cd tempest
sbt assembly
java -Xmx2g -cp target/scala-2.11/bidirectional-random-walk-assembly-1.0.jar\
  co.teapot.graph.MemoryMappedDirectedGraphConverter \
  src/test/resources/test_graph.txt test_graph.dat
```
 
Then in Scala you can then create a [MemoryMappedDirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.MemoryMappedDirectedGraph)
and call methods as specified in [DirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.DirectedGraph).  For
 example:
```
sbt console
val graph = co.teapot.graph.MemoryMappedDirectedGraph("test_graph.dat")
graph.nodeCount
graph.edgeCount
graph.outDegree(4)
graph.outNeighbors(4)
graph.inNeighbors(4)
```

MemoryMappedDirectedGraph is immutable, so we also provide the thread-safe mutable graph class 
[ConcurrentHashMapDynamicGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.ConcurrentHashMapDynamicGraph).  
Finally, for situations where you have a large initial graph but
 want to allow new nodes and edges to be added, use [DynamicDirectedGraphUnion]
 (http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.DynamicDirectedGraphUnion) to combine a memory
mapped graph with a dynamic graph.
