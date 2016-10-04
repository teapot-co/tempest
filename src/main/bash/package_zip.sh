#!/usr/bin/env bash
# Copies all files a client would use (but not the scala source code) into a zip for distribution
set -e
VERSION='0.11.1'

rm -r dist/* || true
mkdir -p dist/tempest

cp -r config dist/tempest/
cp -r bin dist/tempest/
cp -r install dist/tempest/
cp -r example dist/tempest/
cp src/main/python/twitter_2010_example.py dist/tempest/example/twitter_2010_recommendations.py

#cp install/map_edges_linux dist/tempest/bin/map_edges

# Remove annoying files ending with ~
find dist/tempest/ -type f -name '*~' -exec mv '{}' /tmp \;
cp target/scala-2.11/tempest-database-assembly-$VERSION.jar dist/tempest/bin/tempest.jar

src/main/bash/generate_thrift.sh # Updates ruby and python client thrift files
cp python-package/dist/tempest_db-$VERSION.tar.gz dist/tempest/install/
cp ruby-package/tempest_db-$VERSION.gem dist/tempest/install/
#zip -r tempest_$VERSION.zip dist/tempest
cd dist
#For speed, disable tar for now
# tar -cvzf tempest.tgz tempest
