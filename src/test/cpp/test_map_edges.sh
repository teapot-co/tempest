#!/usr/bin/env bash

(cd ../../main/cpp; make)
../../main/cpp/map_edges ../resources/formatting_test_nodes.txt ../resources/formatting_test_edges.txt edges_out.txt
echo "^ should have discarded 2 edges"
echo "diff should be empty:"
diff edges_out.txt ../resources/formatting_test_expected_output.txt
