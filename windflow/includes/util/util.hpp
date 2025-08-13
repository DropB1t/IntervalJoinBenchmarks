/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli and Yuriy Rymarchuk
 *  
 *  This file is part of IntervalJoinBenchmarks.
 *  
 *  IntervalJoinBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/DropB1t/IntervalJoinBenchmarks/blob/main/LICENSE
 *  
 *  IntervalJoinBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#pragma once

#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/istreamwrapper.h>

using namespace std;
using namespace rapidjson;

inline vector<string> split(const string &target, const char delim)
{
    string temp;
    stringstream stringstream{target};
    vector<string> result;

    while (getline(stringstream, temp, delim))
    {
        result.push_back(temp);
    }

    return result;
}

inline void dump_json(Document &doc, const std::string& filename)
{
    std::ofstream fs(filename);
    
    StringBuffer buffer;
    PrettyWriter<StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    fs << buffer.GetString();
}

inline void dump_test_results(Document &doc, const std::string& filename) {
    Document document;
    Document::AllocatorType& allocator = document.GetAllocator();

    std::ifstream ifs(filename);
    if (!ifs) {
        // File doesn't exist, create a new one with an array
        document.SetArray();
    } else {
        // File exists, read the existing data
        IStreamWrapper isw(ifs);
        document.ParseStream(isw);
        if (!document.IsArray()) {
            throw std::runtime_error("Existing file does not contain a JSON array");
        }
    }
    ifs.close();
    
    // Append new data and write it back to the file
    document.PushBack(doc, allocator);

    StringBuffer strbuf;
    PrettyWriter<StringBuffer> writer(strbuf);
    document.Accept(writer);

    std::ofstream ofs(filename);
    if (!ofs.is_open()) {
        throw std::runtime_error("Could not open file for writing");
    }
    ofs << strbuf.GetString();
    ofs.close();
}
