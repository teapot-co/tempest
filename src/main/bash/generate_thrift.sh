#!/usr/bin/env bash
mkdir -p python-package/tempest_graph

thrift -out src/gen/java -gen java src/main/thrift/tempest.thrift
thrift -out src/gen/ruby -gen rb src/main/thrift/tempest.thrift
thrift -out python-package -gen py src/main/thrift/tempest.thrift
cp python-package/tempest_graph_init.py python-package/tempest_graph/__init__.py
