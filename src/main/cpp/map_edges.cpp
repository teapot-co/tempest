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
 * Applies a (id -> person_id) mapping to a file of edges (id1 id2).  First argument is a file of lines of the form
 <id>,<tempest_id>
 Second argument is a file with lines of the form
 <id1>,<id2>
 The mapping is applied to the edges, and lines of the form
 <tempest_id1> <tempest_id2>
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

int main(int argc, char* argv[]) {
    if (argc - 1 != 3) {
        printf("usage: %s <id mapping file> <input edges> <output edges>", argv[0]);
        return -1;
    }

    unordered_map<string, int> idToTempestId;
    auto start = chrono::system_clock::now();
    long long idCount = 0; // Use long long in case there are more than 2**32 ids

    ifstream idStream(argv[1]);
    string line;
    while(getline(idStream, line)) {
        vector<string> parts = split(line, ',');
        if (parts.size() == 2) {
            string id = parts[0];
            int tempestId = stoi(parts[1]);
            //cerr << "Read '" << id << "' -> " << tempestId << "\n";
            idToTempestId[id] = tempestId;
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
    cerr << "Done reading id -> internal id map.\n";
    cerr << "Now mapping edges to internal ids.\n";

    long long lineCount = 0;
    long long discardedEdgeCount = 0;
    start = chrono::system_clock::now();

    ifstream edgeInputStream(argv[2]);
    ofstream edgeOutputStream(argv[3]);
    while(getline(edgeInputStream, line)) {
        vector<string> parts = split(line, ',');
        if (parts.size() == 2) {
            string id1 = parts[0];
            string id2 = parts[1];
            //cerr << "Read '" << id1 << "','" << id2 << "'\n";
            if (idToTempestId.count(id1) > 0 && idToTempestId.count(id2) > 0) {
              edgeOutputStream << idToTempestId[id1] << " " << idToTempestId[id2] << "\n";
            } else {
              discardedEdgeCount++;
              cerr << "Skipping line '" << line << "' for having an id not in the id -> internal id file\n";
            }
            lineCount++;
            if (lineCount % 1000000 == 0) {
              chrono::duration<double> diff = chrono::system_clock::now() - start;
              int edgesPerSecond = (int)(lineCount / diff.count());
              cerr << "Read " << lineCount << " edges.  Average " << edgesPerSecond << " edges per second \n";
            }
        } else if (line.length() > 0){
            cerr << "Skipping line '" << line << "' for not having a single comma\n";
        }
    }
    cerr << "Discarded " << discardedEdgeCount << " edges from " << argv[2] <<
            " for not being in the id -> internal id map " << argv[1] << "\n";
}
