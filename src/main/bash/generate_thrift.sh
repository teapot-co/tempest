#!/usr/bin/env bash
mkdir -p src/gen/java
mkdir -p ruby-package/lib/gen/
mkdir -p python-package/tempest_db

rm -r src/gen/java/* # Remove any old versions
rm -r ruby-package/lib/gen/*
rm -r python-package/tempest_db # Remove any old versions

thrift -out src/gen/java -gen java src/main/thrift/tempest.thrift
thrift -out ruby-package/lib/gen -gen rb src/main/thrift/tempest.thrift
thrift -out python-package -gen py src/main/thrift/tempest.thrift

# Create package
(cd ruby-package; gem build tempest_db.gemspec)

# Thrift over-writes python-package/tempest_db/__init__.py.
# Copy in the correct __init__.py and other python files into the package.
cp src/main/python/*.py  python-package/tempest_db/
(cd python-package; python setup.py sdist > output_python_setup)
