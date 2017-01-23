#!/usr/bin/env bash

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <config file>.yaml"
  exit 1
fi
NODE_TYPE="$1"
CONFIG="/data/config/${NODE_TYPE}.yaml"

if [ ! -f $CONFIG ]; then
  echo "File not found: $CONFIG - Please place config file in $CONFIG and retry."
  exit 1
fi

echo "Loading nodes..."
python /root/tempest/system/generate_node_creation_sql.py $CONFIG > "/tmp/load_${NODE_TYPE}.sql"

/root/tempest/system/start_postgres.sh
sudo -Hiu postgres psql tempest postgres -f "/tmp/load_${NODE_TYPE}.sql"
