#!/usr/bin/env bash
# The installer script for Tempest takes a directory as input, and copies over tempest.tgz to the
# directory and unzips it.

set -e # Exit if any command fails

cd $( dirname "${BASH_SOURCE[0]}" )

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <tempest_install_directory>"
  exit 1
fi
INSTALL_DIR=$1

# Create the directory if it doesn't exist.
if [ ! -d $INSTALL_DIR ]; then
  mkdir -p $INSTALL_DIR
fi

tar xvzpf ./tempest.tgz -C $INSTALL_DIR
echo "Tempest is ready in $INSTALL_DIR/tempest"
echo "Next, create a config file similar to $INSTALL_DIR/tempest/example/example_database.yaml  (see the manual for details)"
echo "Then cd to $INSTALL_DIR/tempest and run"
echo "bin/create_database.sh <your config file>"
