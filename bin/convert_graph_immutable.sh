#!/usr/bin/env bash

TEMPEST_PATH="`dirname \"$0\"`/.."

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <source edge text file> <destination binary graph file>"
  exit 1
fi

JAR_FILE="$TEMPEST_PATH/target/scala-2.11/tempest-assembly.jar"
if [ ! -f "$JAR_FILE" ]; then
  echo "Building Tempest"
  sbt assembly
fi

java -Xmx2g -cp $JAR_FILE co.teapot.tempest.graph.MemoryMappedDirectedGraphConverter $1 $2
