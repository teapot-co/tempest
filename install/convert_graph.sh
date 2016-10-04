#!/usr/bin/env bash
# Converts a graph from a list of edges to a binary graph

TEMPEST_PATH="`dirname \"$0\"`/.."

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <source edge text file> <destination binary graph file>"
  exit 1
fi

JAR_FILE="$TEMPEST_PATH/bin/tempest.jar"

java -Xmx64g -cp $JAR_FILE co.teapot.graph.MemMappedDynamicDirectedGraphConverter $1 $2
