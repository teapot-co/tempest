# Tempest: A real-time graph database (Beta)
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Features](#features)
- [Introduction](#introduction)
  - [Graphs are Everywhere](#graphs-are-everywhere)
  - [Applications of Graph Analysis](#applications-of-graph-analysis)
  - [Why a new Graph Database?](#why-a-new-graph-database)
  - [Our Approach](#our-approach)
  - [Created by](#created-by)
- [Using Tempest DB](#using-tempest-db)
  - [Recommended Machine Sizes](#recommended-machine-sizes)
- [Using Tempest as a library](#using-the-tempest-library)
  - [Requirements](#requirements)
  - [Converting a Graph to Tempest Format](#converting-a-graph-to-tempest-format)
  - [Language Support for Tempest](#language-support-for-tempest)
    - [Scala](#scala)
    - [Java](#java)
    - [Python](#python)
- [Project Roadmap](#project-roadmap)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Features
- Single machine, scale-up architecture: Graphs are notoriously hard to partition if you also need to support real-time traversals. So Tempest runs on a single commodity machine with large-ish memory (see [here](#recommended-machine-sizes)).
- Scalable: Despite running on a single machine, Tempest supports graphs with billions of nodes and edges.  For example, it can store a 30 billion edge graph using 300GB of RAM.
- Fast traversal:  Accessing a neighbor of a node is as fast as two RAM accesses. You can run Personalized PageRank in < 200ms.
- High write throughput: Tempest supports about 1000 edge additions per second  when storing them persistently, and hundreds of thousands per second when storing in RAM only.
- Convenient: Tempest is installed with a single Docker command.  Tempest can also be used as a in-memory graph analytics library, and is available as a single sbt or Maven dependency.  
- Fast loading: Tempest loads its graph format instantly, even for 30-billion edge graphs, so you can
  quickly deploy new versions of your code.
- Flexible: Tempest DB supports clients in any language [Thrift](https://thrift.apache.org/)
  supports. We provide client wrappers for Python and Ruby.
  
## Introduction
### Graphs are everywhere
A graph database like Tempest enables you to store and access your graphs the way you conceptually
think of them: as nodes and their connections. Graphs don’t just mean social networks; graphs are 
everywhere. There are communication graphs such as messaging, Skype, and email. There are graphs of
users and products, with each purchase (or view or click) corresponding to an edge in the graph. 
There are interest graphs such as Twitter and Pinterest. There are collaboration graphs such as 
Wikipedia and, increasingly, Google Docs and Office 365. The Internet of Things (IoT) is really a 
graph of connected devices. These graphs are growing, and will continue to grow, both in scale and 
in diversity. Traditional data storage and analytics systems (e.g., SQL, Hadoop, Spark) are poorly 
suited to help you store, access and analyze your graphs. You need a graph database for it.

### Applications of Graph Analysis
Graphs are useful in a variety of search and recommendation problems, for example:
  - Given the search query "John" from a user Alice, find all users named John 
    who are friends-of-friends with Alice.
  - Find all products similar to the ones Jane has bought, where two products are similar if a large
    fraction of people who buy one also buy the other.

A number of graph databases out there perform very poorly on neighborhood and traversal queries. And that performance degrades rapidly as the graph scales. In fact, no graph database out there can be used for real-time queries at the scale of Twitter or Linkedin. That is, no graph database other than Tempest. 

Tempest was purpose-built to enable blazingly fast neighborhood queries. By blazingly fast, we mean in under 250 milliseconds for a graph with 100M nodes and 5B edges.  In fact, Tempest is about 15x faster at these queries than Neo4j. Tempest scales to graphs with up to 4B nodes and 50B edges. It supports 1000 writes (new nodes and edges) per second while supporting truly real-time queries.

### Why a new Graph Database?
At [Teapot](http://teapot.co) we work with large social networks, and we needed a graph library that
could efficiently allow real-time computation on graphs with 10s of billions of edges.  We wanted a
JVM-based library so we could write efficient high-level code in Scala.  Our web backend is in Ruby,
so we also needed a way for languages like Ruby or python to efficiently access our graph.  We
considered using a distributed library like GraphX or Giraph, but those libraries don't support
real-time queries, and the complexity and overhead of managing a cluster were undesirable.  On AWS,
the cost of a machine scales linearly with the amount of RAM used, so for the price of a 256GB
cluster, we could rent a single 256GB machine, and have the convenience and performance of running
Scala code directly on a single machine.  We initially used the [Cassovary](https://github.com/twitter/cassovary) library, 
but as our graph grew beyond a few billion edges, Cassovary was taking hours to load our graph,
making deployments difficult without significant downtime to our service.  This led us to rethink
how we store graphs.

### Our Approach
A key feature of Tempest is reading data directly from a memory mapped binary file, so there is no
parsing required and no object or garbage collector overhead.  Once the graph file is cached in RAM
by the OS, Tempest can read the graph instantly rather than load the graph at start-up as other
libraries would.  This makes deploying code changes quick and easy.
It requires only 8 bytes per edge for bi-directional edge access, so, for example, it can store the
[Twitter-2010](http://law.di.unimi.it/webdata/twitter-2010/) graph of 1.5 billion edges using 
12GB of RAM.  We use the Tempest library in production, and it has careful unit tests.

### Created by
Tempest was built by a team of Stanford PhDs---[Peter Lofgren](@plofgren) (lead developer), [Ashish Goel](@ashishgoel), [Pranav Dandekar](@dandekar)---who have built state-of-the-art systems and algorithms for large scale network analytics at Stanford, Twitter and Amazon. If you have any questions about using Tempest or have feature requests, create a Github issue or send an email to <peter.lofgren@cs.stanford.edu>.


## Using Tempest DB (alpha)
1. Tempest's dependencies (including postgres and java) are neatly 
   packaged in a Docker container.  Tempest stores your data in a given 
   directory outside docker, so you can upgrade tempest without losing your data.  To
   use Tempest DB, install docker on your machine, then run
   
   `docker run -t -i -v $YOUR_DATA_DIR:/data -p 127.0.0.1:10001:10001 teapotco/tempestdb:latest bash`
   
   where `$YOUR_DATA_DIR` is a directory on your machine with the graph you want to import and which will also store your
   config files.
   You can make additional directories available to docker as needed;
   for example, to make your home directory available from inside docker, add `-v ~/:/mnt/home/` to the above command. 
   The docker image contains a built release of Tempest. 
   If you're a developer and would like to run against
   a tempest repo you've checked out locally (say at ~/tempest), add `-v ~/tempest/:/root/tempest/` to the above command.
   For development, you should also add `-p 5432:5432` to the above command, and follow the below instructions
   for creating the test node and edge types, so that your tests can run outside docker against the postgres database inside docker.
2. You need three types of file to fire up the Tempest server with your graph: 
   1. a csv file for each node type,
   2. a csv file for each edge type, and
   3. a folder of config files (one file for each node type and each edge type).

   To see the format of the headerless node csv and the edge csv expected by Tempest, look at `example/users.csv`
   and `follows.csv`.
3. Once you have your node and edge files in csv format, create a config file in `$YOUR_DATA_DIR/config/`
   for each node and edge type.  The name of the config file must match the name of the node or edge type,
   so for example if you have a node type called `user` there must be a file in `$YOUR_DATA_DIR/config/user.yaml`.
   As in the example files `example/user.yaml` and `example/book.yaml`, each `<node_type>.yaml` file should have the following fields:
   - csvFile: the headerless csv file, for example "/mnt/home/data/users.csv"
   - nodeAttributes A list of name and type for each attribute in your csv file. Attributes type 
      may only be 'string', 'int' (32 bit), 'bigint' (64 bit), or 'boolean'. Enter the attributes
      in the same order as they appear in the node file.  One of your nodeAttributes must be called 'id'. 
      This nodeAttribute must have type string, and must be unique across all nodes of this type.
   As in the example files `example/follows.yaml` and `example/has_read.yaml`, each `<edge_type>.yaml` file should have the following fields:
   - csvFile: the headerless csv file, for example '/mnt/home/data/has_read.csv'.  Every line of this file must be a pair "sourceId,targetId"
      where sourceId matches some id of sourceNodeType, and targetId matches the id of some node in targetNodeType
   - sourceNodeType: The type of node on the left of each edge, for example 'user'
   - targetNodeType: The type of node on the right of each edge, for example 'book'
4. Convert your graph to binary and load your nodes and edges into Postgres by running
   `create_node_type.sh <node_type>` or `create_edge_type.sh <edge_type>` for each node or edge type.
   For example, to load the example graphs, from inside docker run
   
   ```
   # Note: normally you'd create config files outside docker, directly in $YOUR_DATA_DIR
   mkdir -p /data/config/
   cp /root/tempest/example/{user,book,follows,has_read}.yaml /data/config/
   chmod 777 /data/config/
   create_node_type.sh user
   create_node_type.sh book
   create_edge_type.sh follows
   create_edge_type.sh has_read
   ```
   
   Depending on the size of your initial graph, this step may take up to a few hours. For example, for a graph of 1B edges, this step will take about 4 hours. We realize that is a long time to wait to get your hands on Tempest, but this is one-time hassle: once Tempest is initialized, stopping/starting only takes a few seconds.
5. Start the server with
   `start_server.sh`
6. Now you can connect to the server from inside or outside docker.  If outside docker, first run 
   `pip install tempest_db`. Then in `ipython` you can run, for example
   
   ```
   import tempest_db
   client = tempest_db.TempestClient()
   alice = tempest_db.Node("user", "alice")
   client.out_neighbors("follows", alice)
   # result: [Node(type='user', id='bob')]
   alice_books = client.out_neighbors("has_read", alice)
   client.multi_node_attribute(alice_books, "title")
   # result: {Node(type='book', id='101'): 'The Lord of the Rings',
   #          Node(type='book', id='103'): 'Roots'}
   ```

### Recommended Machine Sizes

|Number of edges in the graph | EC2 Instance type | Volume size (in GB) |
|-----------------------------|-------------------|--------------------|
| < 50M    | r3.large   | 100   |
| 50M-500M | r3.xlarge  | 200   |
| 500M-5B  | r3.2xlarge | 2000  |
| 5B-10B   | r3.4xlarge | 5000  |
| 10B+     | r3.8xlarge | 10,000|

## Using Tempest as a library

### Requirements
Tempest depends on Java, [sbt](http://www.scala-sbt.org/download.html), and [thrift](https://thrift.apache.org/).  To install thrift on OS X, you can run `brew install thrift`, or for other platforms see their [install instructions](https://thrift.apache.org/docs/install/). Before compiling Tempest, you'll need to generate thrift files using `src/main/bash/generate_thrift.sh`.

### Converting a Graph to Tempest Format
For large graphs, you will want to first convert your graph to the Tempest binary format.  The 
input to our converter is a simple text format where each line has the form "id1 id2" to 
indicate an edge from id1 to id2, where id1 and id2 are integers between 0 and 2^31.  To convert 
the test graph, for example, run:
```
git clone https://github.com/teapot-co/tempest.git
cd tempest
src/main/bash/generate_thrift.sh
bin/convert_graph_immutable.sh src/test/resources/test_graph.txt test_graph.dat
```
### Languages Supported by Tempest
#### Scala 
Simply add
```
libraryDependencies += "co.teapot" %% "tempest" % "0.14.0"
```
to your `build.sbt` file.  The graph classes in tempest are the following:

- [DirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.tempest.graph.DirectedGraph)
  defines the interface for graphs
- [MemoryMappedDirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.tempest.graph.MemoryMappedDirectedGraph)
  efficiently reads an immutable graph from a Tempest binary graph file
- [MemoryMappedMutableDirectedGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.tempest.graph.MemMappedDynamicDirectedGraph)
  efficiently stores a large graph in a binary file and allows efficient edge additions
- [ConcurrentHashMapDynamicGraph](http://teapot-co.github.io/tempest/scaladoc/#co.teapot.tempest.graph.ConcurrentHashMapDynamicGraph)
  is a simple mutable graph class based on ConcurrentHashMap
    
As an example of using the graph methods, convert the test graph to test_graph.dat as described 
above, then run the following from the tempest directory:
```
sbt console
val graph = co.teapot.tempest.graph.MemoryMappedDirectedGraph("test_graph.dat")
graph.nodeCount
graph.edgeCount
graph.outDegree(4)
graph.outNeighbors(4)
graph.inNeighbors(4)
```

#### Java
Because scala compiles to the jvm, you can naturally use Tempest from Java.  Tempest is in a maven repo,
so you're using maven, simply add
```
<dependency>
      <groupId>co.teapot</groupId>
      <artifactId>tempest_2.11</artifactId>
      <version>0.14.0</version>
    </dependency>
```
to your pom.xml file to access the Tempest dependency.  Then for example, in Java you can 
`import co.teapot.tempest.graph.MemoryMappedDirectedGraph` and write
```
MemoryMappedDirectedGraph graph = new MemoryMappedDirectedGraph(new File("test_graph.dat"));
System.out.println("out-neighbors of 2: " + graph.outNeighborList(2));
```
The documentation javadoc for the graph classes is linked above in the Scala section.

#### Python
To use tempest from python, it has a client/server mode, where the server
stores large graphs in RAM, while the client is conveniently in python.  The server and client can even
be on different machines; for example the client can run on a web server while the server runs in a large
graph server.  See the above instructions on using TempestDB.

## Project Roadmap
- Support edge attributes.
