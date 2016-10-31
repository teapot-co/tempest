#!/usr/bin/env bash
# Stops the tempest server

export TEMPEST_DIR=/root/tempest

if [ ! -f $TEMPEST_DIR/tempest.pid ]; then
  echo "Tempest does not seem to be running: tempest.pid does not exist."
  exit 1
fi

sudo service postgresql stop

TEMPEST_PID="$(cat $TEMPEST_DIR/tempest.pid)"
echo "Stopping Tempest server..."
kill $TEMPEST_PID
rm $TEMPEST_DIR/tempest.pid
echo "Tempest server stopped."
