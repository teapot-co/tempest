TODO: Update readme for database.

# Tempest Graph Library
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Background](#background)
- [Requirements](#requirements)
- [Converting a Graph to Tempest Format](#converting-a-graph-to-tempest-format)
- [Using Tempest from Scala](#using-tempest-from-scala)
- [Using Tempest from Java](#using-tempest-from-java)
- [Using Tempest from Python](#using-tempest-from-python)
- [Project Roadmap](#project-roadmap)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Background
At [Teapot](http://teapot.co) we work with large social networks, and we needed a graph library that
could efficiently allow real-time computation on large graphs.  We wanted a JVM-based library so we
could write efficient high-level code in Scala.  Our web backend is in Ruby, so we also needed a way
for languages like Ruby or python to efficiently access our graph.  We considered using a
distributed library like GraphX or Giraph, but those libraries don't support real-time queries, and
the complexity and overhead of managing a cluster were undesirable.  On AWS, the cost of a machine
scales linearly with the amount of RAM used, so for the price of a 256GB cluster, we could rent a
single 256GB machine, and have the convenience and performance of running Scala code directly on a
single machine.  We initially used the [Cassovary](https://github.com/twitter/cassovary) library,
which we liked, but as our graph grew the loading time became significant.  Cassovary was taking
hours to load our graph from a text file, making deployments difficult without significant downtime
to our service.  This led us to rethink how to store graphs.

Tempest is a graph library for efficiently handling large graphs.  A key feature of Tempest is
reading data directly from a memory mapped binary file, so there is no parsing required and no
object or garbage collector overhead.  Once the graph file is cached in RAM by the OS, Tempest can 
read the graph instantly rather than load the graph as other libraries would.  
It requires only 8 bytes per edge for bi-directional edge access, so, for example, it can store the
[Twitter-2010](http://law.di.unimi.it/webdata/twitter-2010/) graph of 1.5 billion edges using 
12GB of RAM.  We use Tempest in production, and it has careful unit tests.

If you have any questions about using Tempest or have feature requests, create a Github issue or 
send me an email at <peter.lofgren@cs.stanford.edu>.

## Requirements
Tempest depends on Java and [sbt](http://www.scala-sbt.org/download.html).

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
Simply add
```
libraryDependencies += "co.teapot" %% "tempest" % "0.10.0"
```
to your `build.sbt` file.  The graph classes in tempest are the following:

- [DirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.DirectedGraph)
  contains the methods defined for graphs
- [MemoryMappedDirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.MemoryMappedDirectedGraph)
  efficiently creates an immutable graph from a Tempest binary graph file
- [ConcurrentHashMapDynamicGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.ConcurrentHashMapDynamicGraph)
  is a thread-safe mutable graph class based on ConcurrentHashMap
- [DynamicDirectedGraphUnion](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.graph.DynamicDirectedGraphUnion)
  combines an immutable graph with a mutable graph, for situations where you have a large 
  memory mapped graph but want to be able to add edges to it.
    
As an example of using the graph methods, convert the test graph to test_graph.dat as described 
above, then run the following from the tempest directory:
```
sbt console
val graph = co.teapot.graph.MemoryMappedDirectedGraph("test_graph.dat")
graph.nodeCount
graph.edgeCount
graph.outDegree(4)
graph.outNeighbors(4)
graph.inNeighbors(4)
```

## Using Tempest from Java
Because scala compiles to the jvm, you can naturally use Tempest from Java.  Tempest is in a maven repo,
so you're using maven, simply add
```
<dependency>
      <groupId>co.teapot</groupId>
      <artifactId>tempest_2.11</artifactId>
      <version>0.10.0</version>
    </dependency>
```
to your pom.xml file to access the Tempest dependency.  Then for example, in Java you can 
`import co.teapot.graph.MemoryMappedDirectedGraph` and write
```
MemoryMappedDirectedGraph graph = new MemoryMappedDirectedGraph(new File("test_graph.dat"));
System.out.println("out-neighbors of 2: " + graph.outNeighborList(2));
```
The documentation javadoc for the graph classes is linked above in the Scala section.

## Using Tempest from Python
To use tempest from python, it has a client/server mode, so the server (written efficiently in scala)
stores large graphs in RAM, while the client is conveniently in python.  The server and client can even
be on different machines; for example the client can run on web server while the server runs in a large
graph server. First clone the repository and convert the graph to Tempest binary format, as described
 above.  Then start the server, supplying the binary graph name, for example
```
bin/start_server.sh test_graph.dat 
```
The server runs by default on TCP port 10001, but you can change this, for example
```
bin/start_server.sh test_graph.dat 12345
```

Finally,  connect to the server from python.  First run `pip install tempest_graph`.  Then
from the python console:
```
>>> import tempest_graph
>>> graph = tempest_graph.get_client()
>>> graph.maxNodeId()
9
>>> graph.outDegree(2)
4
>>> graph.outNeighbors(2)
[0, 1, 3, 9]
```

## Project Roadmap
- Ruby bindings
- Implement clustering algorithms
- (potentially) Implement PageRank, personalized PageRank, or shortest path algorithms
- (potentially) Implement persistent store for edge updates
- (potentially) Support node and edge attributes
- (potentially) Support non-integer node ids
