#!/usr/bin/env bash
# Converts a graph from a list of int pair edges to a binary graph

TEMPEST_PATH="`dirname \"$0\"`/.."

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <source edge text file> <destination binary graph file>"
  exit 1
fi

JAR_FILE="$TEMPEST_PATH/target/scala-2.11/tempest-assembly.jar"

java -Xmx64g -cp $JAR_FILE co.teapot.tempest.graph.MemMappedDynamicDirectedGraphConverter $1 $2
