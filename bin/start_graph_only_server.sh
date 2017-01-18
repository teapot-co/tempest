#!/usr/bin/env bash

TEMPEST_PATH="`dirname \"$0\"`/.."

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <binary graph file> <optional tcp port>"
  exit 1
fi

GRAPH=$1
PORT=${2:-10001}

JAR_FILE="$TEMPEST_PATH/target/scala-2.11/tempest-assembly.jar"
if [ ! -f "$JAR_FILE" ]; then
  echo "Building Tempest"
  sbt assembly
fi

java -cp $JAR_FILE co.teapot.tempest.server.TempestServer \
  -conf $GRAPH -port $PORT
