/*
 * Copyright 2016 Teapot, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

/*
Applies a pair of (identifier -> int id) mappings to a file of edges (identifier1 identifier2).
Arguments:
  1. A file with lines of the form
    <identifier1>,<id1>
  2. A file with lines of the form
    <identifier2>,<id2>
  3. An input edge file with lines of the form
    <identifier1>,<identifier2>
  4. The output edge file, where we will write lines of the form
    <id1>,<id2>
First argument is a file with lines of the form
  <identifier1>,<id1>
 Second argument is a file with lines of the form
 <id1>,<id2>
 The mapping is applied to the edges, and lines of the form
 <id1> <id2>
 are written to a file specified by third argument.
 */

#include <unordered_map>
#include <iostream>
#include <fstream>
#include <chrono>
#include <string>
#include <sstream>
#include <vector>
#include <cctype>
#include <locale>
#include <algorithm>

using namespace std;

// trim function based on http://stackoverflow.com/questions/216823/whats-the-best-way-to-trim-stdstring?page=1&tab=votes#tab-top
bool shouldKeepCharacter(int c) {
  return !(isspace(c) || c == '"');
}
static void trim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), shouldKeepCharacter));
    s.erase(std::find_if(s.rbegin(), s.rend(), shouldKeepCharacter).base(), s.end());
}

// split function is from http://stackoverflow.com/questions/236129/split-a-string-in-c
std::vector<std::string> split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        trim(item);
        elems.push_back(item);
    }
    return elems;
}

std::unordered_map<string, int> parseIdentifierToIdFile(char* path) {
    unordered_map<string, int> identifierToId;
    auto start = chrono::system_clock::now();
    long long idCount = 0; // Use long long in case there are more than 2**32 ids

    cerr << "Reading identifier -> id map from " << path << endl;
    ifstream idStream(path);
    string line;
    while(getline(idStream, line)) {
        vector<string> parts = split(line, ',');
        if (parts.size() == 2) {
            string id = parts[0];
            int tempestId = stoi(parts[1]);
            //cerr << "Read '" << id << "' -> " << tempestId << "\n";
            identifierToId[id] = tempestId;
            idCount++;
            if (idCount % 1000000 == 0) {
              chrono::duration<double> diff = chrono::system_clock::now() - start;
              int idsPerSecond = (int)(idCount / diff.count());
              cerr << "Read " << idCount << " (id, internal id) pairs.  Average " << idsPerSecond << " ids per second \n";
            }
        } else if (line.length() > 0){
           cerr << "Skipping line '" << line << "' for not having a single comma\n";
        }
    }
    chrono::duration<double> diff = chrono::system_clock::now() - start;
    cerr << "Reading " << idCount << "(identifier, id) pairs from " << path << " took " << diff.count() << " seconds.\n";
    return identifierToId;
}

int main(int argc, char* argv[]) {
    if (argc - 1 != 4) {
        printf("usage: %s <(identifier, id) file 1> <(identifier, id) file 2> <input identifier edges> <output id edges>", argv[0]);
        return -1;
    }

    unordered_map<string, int> identifierToId1 = parseIdentifierToIdFile(argv[1]);
    unordered_map<string, int> identifierToId2 = parseIdentifierToIdFile(argv[2]);
    ifstream edgeInputStream(argv[3]);
    ofstream edgeOutputStream(argv[4]);

    cerr << "Now mapping edges to internal ids.\n";
    long long lineCount = 0;
    long long discardedEdgeCount = 0;
    auto start = chrono::system_clock::now();
    string line;

    while(getline(edgeInputStream, line)) {
        vector<string> parts = split(line, ',');
        if (parts.size() == 2) {
            string identifier1 = parts[0];
            string identifier2 = parts[1];
            //cerr << "Read '" << id1 << "','" << id2 << "'\n";
            if (identifierToId1.count(identifier1) > 0 && identifierToId2.count(identifier2) > 0) {
              edgeOutputStream << identifierToId1[identifier1] << " " << identifierToId2[identifier2] << "\n";
            } else {
              discardedEdgeCount++;
              cerr << "Skipping line '" << line << "' for having an id not in the identifier -> id file\n";
            }
            lineCount++;
            if (lineCount % 1000000 == 0) {
              chrono::duration<double> diff = chrono::system_clock::now() - start;
              int edgesPerSecond = (int)(lineCount / diff.count());
              cerr << "Read " << lineCount << " edges.  Average " << edgesPerSecond << " edges per second \n";
            }
        } else if (line.length() > 0) {
            cerr << "Skipping line '" << line << "' for not having a single comma\n";
        }
    }
    cerr << "Discarded " << discardedEdgeCount << " of " << lineCount << " edges for not being in both identifier maps\n";
}
