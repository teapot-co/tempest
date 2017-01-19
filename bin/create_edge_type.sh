#!/usr/bin/env bash

set -e # Exit on first error

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <edge type>"
  echo "where <edge type>.yaml is a file in /data/config/"
  exit 1
fi
EDGE_TYPE="$1"

CONFIG="/data/config/${EDGE_TYPE}.yaml"

if [ ! -f $CONFIG ]; then
  echo "File not found: $CONFIG - Please place config file in $CONFIG and retry."
  exit 1
fi

BINARY_EDGE_FILE="/data/binary_graphs/${EDGE_TYPE}.dat"
if [ -f $BINARY_EDGE_FILE ]; then
  read -p "Edge file $BINARY_EDGE_FILE exists -- are you sure you want to overwrite it? " -n 1 -r
  echo # move to a new line
  if [[ $REPLY =~ ^[Yy]$ ]]
  then
    rm $BINARY_EDGE_FILE
  else
    exit 1
  fi
fi

NODE_TYPE1=$(/root/tempest/system/get_yaml_field.py $CONFIG sourceNodeType)
NODE_TYPE2=$(/root/tempest/system/get_yaml_field.py $CONFIG targetNodeType)
NODE_IDENTIFIER1=$(/root/tempest/system/get_yaml_field.py $CONFIG sourceNodeIdentifierField)
NODE_IDENTIFIER2=$(/root/tempest/system/get_yaml_field.py $CONFIG targetNodeIdentifierField)
CSV_FILE=$(/root/tempest/system/get_yaml_field.py $CONFIG csvFile)


echo "Creating edge identifier -> id files..."
ID_MAP1="/tmp/${NODE_TYPE1}_${NODE_IDENTIFIER1}_id.csv"
ID_MAP2="/tmp/${NODE_TYPE2}_${NODE_IDENTIFIER2}_id.csv"
# TODO (optimization): Don't create id map if the node identifier field is already id
/root/tempest/system/start_postgres.sh
sudo -Hiu postgres psql tempest postgres -c "COPY (SELECT ${NODE_IDENTIFIER1}, id FROM ${NODE_TYPE1}_nodes) TO '${ID_MAP1}' DELIMITER ',' CSV"
sudo -Hiu postgres psql tempest postgres -c "COPY (SELECT ${NODE_IDENTIFIER2}, id FROM ${NODE_TYPE2}_nodes)  TO '${ID_MAP2}' DELIMITER ',' CSV"


echo "Relabeling edges to ids..."
/root/tempest/system/map_edges_linux \
  "${ID_MAP1}" \
  "${ID_MAP2}" \
  "${CSV_FILE}" \
  "/tmp/${EDGE_TYPE}_mapped_edges.csv"

echo "Converting edges to an efficient binary format..."
mkdir -p /data/binary_graphs
/root/tempest/bin/convert_graph.sh "/tmp/${EDGE_TYPE}_mapped_edges.csv" $BINARY_EDGE_FILE

# If we later have applications where we want a persistent backup of edges to postgres, we can uncomment this.
#echo "Backing up edges to Postgres in background..."
#sudo -Hiu postgres psql tempest postgres -c "
#DROP TABLE IF EXISTS ${GRAPH}_edges;
#CREATE TABLE ${GRAPH}_edges
#(id1 bigint, id2 bigint);
#ALTER TABLE ${GRAPH}_edges OWNER TO tempest;
#COPY ${GRAPH}_edges (id1, id2) FROM '${MAPPED_EDGE_PATH}' DELIMITER ' ' CSV;
#"  &
# We don't currently need indexes
#CREATE INDEX ON edges (id1);
#CREATE INDEX ON edges (id2);

echo "Done creating binary graph for edges in ${CSV_FILE}!"
