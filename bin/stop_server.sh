#!/usr/bin/env bash
# Stops a tempest server using the config in the config file passed in as an arg.

#TODO: If we want to support both a given config file (relative to the current dir) and
# a default config file (relative to the tempest install dir), uncomment and test the following:
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <config_file>.yaml [<port>]"
  exit 1
fi

# cd to tempest dir to allow relative links
cd $( dirname "${BASH_SOURCE[0]}" ); cd "../"
export TEMPEST_DIR=`pwd`

if [ ! -f $TEMPEST_DIR/tempest.pid ]; then
  echo "Tempest does not seem to be running: tempest.pid does not exist."
  exit 1
fi

if pgrep "postgres" > /dev/null; then
sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -w -D "${TEMPEST_DIR}/postgres_data" stop -m fast
fi

TEMPEST_PID="$(cat $TEMPEST_DIR/tempest.pid)"
echo "Stopping Tempest server..."
kill -9 $TEMPEST_PID
rm $TEMPEST_DIR/tempest.pid
echo "Tempest server stopped."
