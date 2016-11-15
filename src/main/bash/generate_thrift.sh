#!/usr/bin/env bash
mkdir -p src/gen/java
mkdir -p python-package/tempest_graph
mkdir -p python-package/tempest_db
mkdir -p ruby-package/lib/gen/

rm -r src/gen/java/* # Remove any old versions
rm -r ruby-package/lib/gen/*

thrift -out ruby-package/lib/gen -gen rb src/main/thrift/tempest.thrift
thrift -out src/gen/java -gen java src/main/thrift/tempest.thrift
thrift -out src/gen/java -gen java src/main/thrift/tempest_db.thrift

thrift -out ruby-package/lib/gen -gen rb src/main/thrift/tempest_db.thrift

# python package
rm -r python-package/tempest_db # Remove any old versions
mkdir -p python-package/tempest_db
thrift -out python-package/tempest_db -gen py src/main/thrift/tempest.thrift
thrift -out python-package -gen py src/main/thrift/tempest_db.thrift

# Thrift over-writes python-package/tempest_db/__init__.py. Copy in the correct __init__.py
# and other python files into the package.
cp src/main/python/*.py  python-package/tempest_db/
(cd python-package; python setup.py sdist > output_python_setup)

# Ruby package
(cd ruby-package; gem build tempest_db.gemspec)
