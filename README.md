# Tempest Graph Library

## Background
At [Teapot](http://teapot.co) we work with large social networks, and we needed a graph library that
 could efficiently allow real-time computation on large graphs.  We wanted a JVM-based library so
  we could write efficient high-level code in Scala.  Our web backend is in Ruby, so we also 
  needed a way for languages like Ruby or python to efficiently access our graph.  We considered 
  using a distributed library like GraphX or Giraph, but those libraries don't support 
  real-time queries, and the complexity and overhead of managing a cluster were undesirable.  On 
  AWS, the cost of a machine scales linearly with the amount of RAM used, so for the price of a 
  256GB cluster, we could rent a single 256GB machine, and have the convenience and performance 
  of running scala code directly on a single machine.  We initially used the 
  high-quality [Cassovary](https://github.com/twitter/cassovary) library, but as our graph grew 
  the loading time became significant.  Cassovary was taking hours to load our graph from a text 
  file, making deployments difficult without significant downtime to our service.  This led us to
  rethink how to store graphs.

Tempest is a graph library for efficiently handling large graphs.  A key feature of Tempest is
reading data directly from a memory mapped binary file, so there is no parsing required and no
object or garbage collector overhead.  Once the graph file is cached in RAM by the OS, programs
using tempest can read the graph instantly rather than load the graph as other libraries would.  
It requires only 8 bytes per edge for bi-directional edge access, so, for example, it can store the
[Twitter-2010](http://law.di.unimi.it/webdata/twitter-2010/) graph of 1.5 billion edges using 
12GB of RAM.  

If you have any questions about using Tempest or have feature requests, create a Github issue or 
send me an email at <peter.lofgren@cs.stanford.edu>.

## Project Roadmap
- Implement Python and Ruby bindings
- Implement clustering algorithms
- (if requested) Implement persistent store for edge updates
- (if requested) Support node and edge attributes
- (if requested) Support non-integer node ids

## Requirements
Tempest requires Java and [sbt](http://www.scala-sbt.org/download.html).

## Converting a Graph to Tempest Format
For large graphs, you will want to first convert your graph to the Tempest binary format.  The 
input to our converter is a simple text format where each line has the form "id1 id2" to 
indicate an edge from id1 to id2, where id1 and id2 are integers between 0 and 2^31.  To convert 
the test graph, for example, run:
```
git clone https://github.com/teapot-co/tempest.git
cd tempest
bin/convert_graph.sh src/test/resources/test_graph.txt test_graph.dat
```

## Using Tempest from Scala 
The graph classes in tempest are the following:
  - [DirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.DirectedGraph) -
   contains the methods defined for graphs
  - [MemoryMappedDirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.MemoryMappedDirectedGraph)
   efficiently creates an immutable graph from a Tempest binary graph file
  - [ConcurrentHashMapDynamicGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.ConcurrentHashMapDynamicGraph)
    is a thread-safe mutable graph class based on ConcurrentHashMap
  - [DynamicDirectedGraphUnion](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.DynamicDirectedGraphUnion)
    combines an immutable graph with a mutable graph, for situations where you have a large 
    memory mapped graph but want to be able to add edges to it.
    
As an example of using the graph methods, convert the test graph to test_graph.dat as described 
above, then run the following:
```
sbt console
val graph = co.teapot.graph.MemoryMappedDirectedGraph("test_graph.dat")
graph.nodeCount
graph.edgeCount
graph.outDegree(4)
graph.outNeighbors(4)
graph.inNeighbors(4)
```
