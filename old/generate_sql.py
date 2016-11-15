#!/usr/bin/env python
# Given a config file like example_database.yaml, this generates the sql statements to
# 1) create the postgres database and nodes table
# 2) import the nodes CSV file
# 3) re-map the edge file to sequential int tempest ids

import sys
import yaml
import os
type_to_postgres = {
    "int": "int",
    "bigint": "bigint",
    "string": "varchar",
    "boolean": "boolean"
    }
    
if len(sys.argv) != 2:
  print "Usage: python " + sys.argv[0] + " <graph>.yaml"
  sys.exit(1)
config_filename = sys.argv[1]

with open(config_filename, 'r') as config_file:
  try:
    config = yaml.safe_load(config_file)
    graph_name = config["graph_name"]
    table_name = graph_name + "_nodes"
    id_map_filename = "/tmp/" + graph_name + "_id_map.csv"
    node_csv = os.path.join("/root/tempest/", config['node_file'])

    # Attributes are given as a list of single-entry maps
    node_attribute_maps = config['node_attributes']
    for m in node_attribute_maps:
        assert(len(m) == 1)
    node_attributes = [m.keys()[0] for m in node_attribute_maps]
    node_attribute_types = [m.values()[0] for m in node_attribute_maps]
    # If the imported data doesn't have a "id" column, we'll create one.
    create_id = ("id" not in node_attributes)

    print "DROP TABLE IF EXISTS " + table_name + ";"
    print "CREATE TABLE " + table_name + " ("
    if create_id:
        print "    id SERIAL, "
    else:
        assert node_attributes["id"] == "int", "id must have type int"
    for (attribute, attribute_type) in zip(node_attributes, node_attribute_types):
        if attribute_type in type_to_postgres:
            postgres_type = type_to_postgres[attribute_type]
            print '    "' + attribute + '" ' + postgres_type + ","
        else:
            print "Invalid attribute type \"" + attribute_type + "\" in " + config_filename
            sys.exit(1)
    print "    json_attributes jsonb"
    print ");"

    print "ALTER TABLE " + table_name + " OWNER TO tempest;"
    print "ALTER TABLE " + table_name + " ALTER id SET NOT NULL;"
    if "node_file" in config:
      attribute_list = "(" + ", ".join(node_attributes) + ")"
      print "COPY " + table_name + " " + attribute_list + " FROM '" + node_csv + \
            "' DELIMITER ',' QUOTE '\"' ESCAPE '\\' CSV;"
      
    for attribute in node_attributes:
        print "CREATE INDEX ON " + table_name + " (" + attribute + ");"

    # If importing edges, pull new tempest ids from the node attributes table
    # so the edge formatter can use them.
    if "edge_file" in config:
      print "COPY " + table_name + " (" + config["node_identifier_field"] + ", id) TO '" + \
            id_map_filename + "'  DELIMITER ',' CSV;"

  except yaml.YAMLError as exc:
    print(exc)
    sys.exit(1)
