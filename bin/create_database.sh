#!/usr/bin/env bash
# Given a config file like example/example_database.yaml which describes input csv files and schema,
# this script creates a Tempest database, imports the node and edge data from the csv files,
# and converts the edges to an internal binary format.  Once this script completes a Tempest server
# can connect to the new database and serve requests.

set -e # Exit if any command fails

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <config file>.yaml"
  exit 1
fi
CONFIG=$1

# cd to tempest dir to allow relative links
cd $( dirname "${BASH_SOURCE[0]}" ); cd "../"
export TEMPEST_DIR=`pwd`

export DATABASE='tempest' # For simplicity we now require the database to be named TEMPEST.
export SHARED_BUFFERS=`install/get_yaml_field.py $CONFIG database_caching_ram`


# If Postgres data folder isn't set up, set it up now
if [ ! -d "postgres_data" ]; then
  mkdir postgres_data
  sudo chown postgres:postgres postgres_data
  sudo -u postgres /usr/lib/postgresql/9.5/bin/initdb "${TEMPEST_DIR}/postgres_data"

  sudo touch postgres_log
  sudo chown postgres:postgres postgres_log

  # If postgres is running here, it means it is the "default" postgres.
  if pgrep "postgres" > /dev/null; then
    sudo service postgresql stop
  fi

  sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -w -D "${TEMPEST_DIR}/postgres_data" -l "${TEMPEST_DIR}/postgres_log" start

  # Allow tempest local login without password
  echo "Allowing local java process to connect to postgres"
  HBA=`sudo -u postgres psql -t -P format=unaligned -c 'show hba_file'`
  sudo -u postgres sed -i '1i host    all             tempest             127.0.0.1/32            trust' $HBA
  sudo -u postgres sed -i '1i local    all             tempest             trust' $HBA
  echo "Creating user tempest"
  sudo -u postgres createuser --createdb --no-createrole --no-superuser tempest
fi

# Only start postgres if one isn't already running.
if ! pgrep "postgres" > /dev/null; then 
  sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -w -D "${TEMPEST_DIR}/postgres_data" -l "${TEMPEST_DIR}/postgres_log" start
fi

# Set postgres RAM buffer size
sudo -u postgres psql -c "ALTER SYSTEM SET shared_buffers = '${SHARED_BUFFERS}'"
#sudo service postgresql stop
sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -w -D "${TEMPEST_DIR}/postgres_data" stop -m fast
echo "Starting postgres"
# TODO: remove temporarily disabled fsync with -o '-F'
sudo -u postgres /usr/lib/postgresql/9.5/bin/pg_ctl -o '-F' -w  -D "${TEMPEST_DIR}/postgres_data" -l "${TEMPEST_DIR}/postgres_log" start

if [ -d "data/$DATABASE/" ]; then
  rm -rf data/$DATABASE/
fi

mkdir -p "tmp"
chgrp postgres tmp
chmod 770 "tmp" # Allow both postgres and user account to write to this folder
mkdir -p "data"

sudo -u postgres dropdb --if-exists $DATABASE
sudo -u postgres createdb $DATABASE

echo "Done creating database!"
echo "To add a graph to your new database, run"
echo "${TEMPEST_DIR}/bin/create_graph.sh <graph config file>"
