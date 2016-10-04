#!/usr/bin/env bash

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <config file>.yaml"
  exit 1
fi
CONFIG=$1


DATABASE='tempest' # For simplicity we now require the database to be named TEMPEST.
export EDGE_FILE=`install/get_yaml_field.py $CONFIG edge_file`
export GRAPH=`install/get_yaml_field.py $CONFIG graph_name`


echo "Loading nodes..."
python install/generate_sql.py $CONFIG > /tmp/load_tables.sql

sudo -u postgres psql $DATABASE postgres -f /tmp/load_tables.sql
#rm /tmp/load_tables.sql For debugging purposes, don't delete load_tables.sql

echo "Renumbering edges to internal ids..."
install/map_edges_linux \
  "data/$DATABASE/id_map.csv" \
  "${EDGE_FILE}" \
  "tmp/${GRAPH}_mapped_edges.csv"

echo "Converting edges to an efficient binary format..."
install/convert_graph.sh "tmp/${GRAPH}_mapped_edges.csv" "data/${GRAPH}_mapped_edges.dat"

MAPPED_EDGE_PATH="`pwd`/data/$DATABASE/mapped_edges.csv"

# TODO: Re-enable this via a config flag
#echo "Loading edges in background..."
#sudo -u postgres psql $DATABASE postgres -c "
#CREATE TABLE edges
#(tempest_id1 bigint, tempest_id2 bigint);
#ALTER TABLE edges OWNER TO tempest;
#COPY edges (tempest_id1, tempest_id2) FROM '${MAPPED_EDGE_PATH}' DELIMITER ' ' CSV;

#"  &
# CREATE INDEX ON edges (tempest_id1);
# CREATE INDEX ON edges (tempest_id2);

echo "Done creating graph $GRAPH!"
echo "To allow connections to Tempest, run "
echo "${TEMPEST_DIR}/bin/start_server.sh"
