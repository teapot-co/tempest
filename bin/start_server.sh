#!/usr/bin/env bash
# Starts a tempest server using the config in (tempest install directory)/config/tempest.yml
# on the default port.

#TODO: If we want to support both a given config file (relative to the current dir) and
# a default config file (relative to the tempest install dir), uncomment and test the following:
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <config_file>.yaml [<port>]"
  exit 1
fi

CONFIG="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
PORT=${2-10001} # TODO: Should port be here or in config file?

# cd to tempest dir to allow relative links
cd $( dirname "${BASH_SOURCE[0]}" ); cd "../"
export TEMPEST_DIR=`pwd`

JAR_FILE="bin/tempest.jar"

if ! ( ps aux | grep postgresql | grep "postgres -D $TEMPEST_DIR" > /dev/null); then
  # If postgres is already running, we can't start
  if pgrep "postgres" > /dev/null; then
    echo "Postgres is already running. Please signal it to shutdown by running"
    echo "sudo killall postgres"
    echo "then re-run $0"
    exit 1
  fi
  # Start postgres
  sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -w -D "${TEMPEST_DIR}/postgres_data" -l "${TEMPEST_DIR}/postgres_log" start
fi

echo "Using config file $CONFIG"
export DATABASE=`install/get_yaml_field.py $CONFIG database_name`
vmtouch -t "data/$DATABASE/mapped_edges.dat" &
java -cp "$JAR_FILE" co.teapot.server.TempestDBServer \
  -conf $CONFIG & # -port $PORT
echo "$!" > "${TEMPEST_DIR}/tempest.pid"
wait
