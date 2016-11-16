#!/usr/bin/env bash
# Starts the tempest server

sudo service postgresql start
while ! service postgresql status | grep "online"; do
    sleep 1
done

# TODO: Read version number from file
JAR_FILE="/root/tempest/target/scala-2.11/tempest-assembly-0.12.0.jar"

vmtouch -t /root/tempest/binary_graphs/*.dat &
java -cp "$JAR_FILE" co.teapot.tempest.server.TempestDBServer &
echo "$!" > "/root/tempest/tempest.pid"

trap 'kill $(jobs -pr)' SIGINT SIGTERM EXIT # Kill background java process when this process is killed
wait
