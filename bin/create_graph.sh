#!/usr/bin/env bash

set -e # Exit on errors

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <config file>.yaml"
  exit 1
fi
CONFIG=$1


DATABASE='tempest' # For simplicity we now require the database to be named tempest.
export EDGE_FILE=`/root/tempest/install/get_yaml_field.py $CONFIG edge_file`
export GRAPH=`/root/tempest/install/get_yaml_field.py $CONFIG graph_name`
export SHARED_BUFFERS=`/root/tempest/install/get_yaml_field.py $CONFIG database_caching_ram`

# Set postgres RAM buffer size
service postgresql start
sudo -u postgres psql -c "ALTER SYSTEM SET shared_buffers = '${SHARED_BUFFERS}'"
service postgresql restart

echo "Loading nodes..."
python /root/tempest/install/generate_sql.py $CONFIG > /tmp/load_tables.sql

sudo -Hiu postgres psql $DATABASE postgres -f /tmp/load_tables.sql
#rm /tmp/load_tables.sql For debugging purposes, don't delete load_tables.sql

echo "Renumbering edges to internal ids..."
/root/tempest/install/map_edges_linux \
  "/tmp/${GRAPH}_id_map.csv" \
  "${EDGE_FILE}" \
  "/tmp/${GRAPH}_mapped_edges.csv"

echo "Converting edges to an efficient binary format..."
MAPPED_EDGE_PATH="/tmp/${GRAPH}_mapped_edges.csv"
mkdir -p /root/tempest/data
# TODO to support multiple graphs, the filename should be ${GRAPH}_mapped_edges.dat
/root/tempest/bin/convert_graph.sh  $MAPPED_EDGE_PATH "/root/tempest/data/mapped_edges.dat"


echo "Backing up edges to Postgres in background..."
sudo -Hiu postgres psql tempest postgres -c "
DROP TABLE IF EXISTS ${GRAPH}_edges;
CREATE TABLE ${GRAPH}_edges
(tempest_id1 bigint, tempest_id2 bigint);
ALTER TABLE ${GRAPH}_edges OWNER TO tempest;
COPY ${GRAPH}_edges (tempest_id1, tempest_id2) FROM '${MAPPED_EDGE_PATH}' DELIMITER ' ' CSV;
"  &
# We don't currently need indexes
#CREATE INDEX ON edges (tempest_id1);
#CREATE INDEX ON edges (tempest_id2);

echo "Done creating graph $GRAPH!"
echo "To allow connections to Tempest, run "
echo "start_server.sh"
