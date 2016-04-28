#!/usr/bin/env bash

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <binary graph file> <output cluster text file> [optional java parameters]"
  exit 1
fi

JAR_FILE="target/scala-2.11/tempest-assembly-0.11.0.jar"
if [ ! -f "$JAR_FILE" ]; then
  echo "Missing Jar file; creating it"
  sbt assembly
fi

java $3 -cp $JAR_FILE co.teapot.clustering.LouvainClustererMain $1 $2
