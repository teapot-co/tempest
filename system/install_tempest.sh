#!/usr/bin/env bash
# Installs Tempest, including python client, postgres, and java.  Assuming we distribute an AMI,
# our clients won't need to run this.

set -e # Exit if any command fails
cd install

# First install pyyaml and generate SQL from config file
echo "Installing python dependencies..."
sudo apt-get install python-pip python-dev ipython
sudo pip install pyyaml
echo "\n\nInstalling python client..."
sudo pip install tempest_db-0.10.0.tar.gz

echo "Installing Java 8"
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install openjdk-8-jdk
#sudo update-alternatives --config java


echo "\n\nInstalling database..."
./install_database.sh
echo "Done installing database!"

echo "To load data, make a file like example_database.yaml, and run, for example,"
echo "bin/create_database.sh install/example_database.yaml"

echo "After creating a database, start the server with"
echo "bin/start_server.sh"
