#! /usr/bin/env python
# Parses a yaml file and returns a field with the given name
from __future__ import print_function
import sys
import yaml

if len(sys.argv) != 3:
  print("Usage: " + sys.argv[0] + " <data_file>.yaml <field name>", file=sys.stderr)
  sys.exit(1)
config_filename = sys.argv[1]
field_name = sys.argv[2]

with open(config_filename, 'r') as config_file:
  try:
    config = yaml.safe_load(config_file)
    if field_name in config:
      print(config[field_name])
    else:
      print("Error: missing field \"" + field_name + "\" in " + config_filename, file=sys.stderr)
      sys.exit(1)
  except yaml.YAMLError as exc:
    print(exc, file=sys.stderr)
    sys.exit(1)
