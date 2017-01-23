#!/usr/bin/env python
# Given a node type config file like example/user.yaml, this generates the sql statements to
# 1) create the node table
# 2) add an auto-increment id field if an id field isn't already present
# 3) import the nodes CSV file

import sys
import yaml
import os
type_to_postgres = {
    "int": "int",
    "bigint": "bigint",
    "string": "varchar",
    "boolean": "boolean",
    "serial": "serial" # Used when id is auto-generated
}

if len(sys.argv) != 2:
    print "Usage: python " + sys.argv[0] + " <node_type_config>.yaml"
    sys.exit(1)
config_filename = sys.argv[1]

with open(config_filename, 'r') as config_file:
    try:
        config = yaml.safe_load(config_file)
        name, _ = os.path.basename(config_filename).rsplit(".")
        table_name = name + "_nodes"
        node_csv = config['csvFile']

        # Attributes are given as a list of single-entry maps
        node_attribute_maps = config['nodeAttributes']
        for m in node_attribute_maps:
            assert(len(m) == 1)
        node_attributes = [m.keys()[0] for m in node_attribute_maps]
        node_attribute_types = [m.values()[0] for m in node_attribute_maps]

        # If the imported data doesn't have a "id" column, we'll create one.
        create_id = ("id" not in node_attributes)

        print "DROP TABLE IF EXISTS " + table_name + ";"
        print "CREATE TABLE " + table_name + " ("
        if create_id:
            print "    id SERIAL PRIMARY KEY,"
            #node_attributes.append("id")
            #node_attribute_types.append("serial")
        else:
            id_index = node_attributes.index("id")
            assert node_attribute_types[id_index] == "int", "id must have type int"
        for (attribute, attribute_type) in zip(node_attributes, node_attribute_types):
            if attribute_type in type_to_postgres:
                postgres_type = type_to_postgres[attribute_type]
                constraint = " PRIMARY KEY" if "attribute" == "id" else ""
                print '    "' + attribute + '" ' + postgres_type + constraint + ","
            else:
                print "Invalid attribute type \"" + attribute_type + "\" in " + config_filename
                sys.exit(1)
        print "    json_attributes jsonb"
        print ");"

        print "ALTER TABLE " + table_name + " OWNER TO tempest;"
        #print "ALTER TABLE " + table_name + " ADD PRIMARY KEY (id);"

        attribute_list = "(" + ", ".join(node_attributes) + ")"
        print "COPY %(table_name)s %(attribute_list)s FROM '%(node_csv)s' "  % locals() + \
              "DELIMITER ',' QUOTE '\"' ESCAPE '\\' CSV;"

        for attribute in node_attributes:
            print "CREATE INDEX ON " + table_name + " (" + attribute + ");"

    except yaml.YAMLError as exc:
        print(exc)
        sys.exit(1)
