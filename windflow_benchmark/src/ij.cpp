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

#include <string>
#include <vector>
#include <sstream>
#include <iostream>

#include <windflow.hpp>
#include <ff/ff.hpp>

#include <fstream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/istreamwrapper.h>

#include "../includes/nodes/source.hpp"
#include "../includes/nodes/join.hpp"
#include "../includes/nodes/sink.hpp"

#include "util/constants.hpp"
#include "util/cli_util.hpp"
#include "util/tuple.hpp"

using namespace std;
using namespace chrono;
using namespace ff;
using namespace wf; // synthetic

typedef enum
{
    UNIFORM_SYNTHETIC,
    ZIPF_SYNTHETIC,
    ROVIO_TEST,
    STOCK_TEST
} test_types;

typedef enum
{
    KEY_BASED,
    DATA_BASED
} test_mode;

atomic<long> sent_tuples; // total number of tuples sent by all the sources
atomic<long> total_bytes; // total number of bytes processed by the system

string outdir;

test_types type;
size_t num_keys = 0;
size_t data_size = 0;

vector<string> split(const string &, const char );

vector<tuple_t> parse_dataset(const string &, const char );

void dumpThroughput(int, const std::string& );

int main(int argc, char *argv[])
{
    /// parse arguments from command line
    int option = 0;
    int index = 0;

    string rpath;
    string lpath;
    test_mode mode;

    size_t rsource_par_deg = 0;
    size_t lsource_par_deg = 0;
    size_t join_par_deg = 0;
    size_t sink_par_deg = 0;

    int64_t lower_bound = 0;
    int64_t upper_bound = 0;

    sent_tuples = 0;
    total_bytes = 0;

    size_t batch_size = 0;
    bool chaining = false;
    long sampling = 0;
    int rate = 0;

    if (argc == 19 || argc == 20) {
        while ((option = getopt_long(argc, argv, "r:k:s:b:p:t:m:l:u:c:o:", long_opts, &index)) != -1) {
            switch (option) {
                case 'r': {
                    rate = atoi(optarg);
                    break;
                }
                case 's': {
                    sampling = atoi(optarg);
                    break;
                }
                case 'b': {
                    batch_size = atoi(optarg);
                    break;
                }
                case 'p': {
                    vector<size_t> par_degs;
                    string pars(optarg);
                    stringstream ss(pars);
                    for (size_t i; ss >> i;) {
                        par_degs.push_back(i);
                        if (ss.peek() == ',')
                            ss.ignore();
                    }
                    if (par_degs.size() != 4) {
                        cout << "Error in parsing the input arguments\n"
                                << endl;
                        exit(EXIT_FAILURE);
                    } else {
                        rsource_par_deg = par_degs[0];
                        lsource_par_deg = par_degs[1];
                        join_par_deg = par_degs[2];
                        sink_par_deg = par_degs[3];
                    }
                    break;
                }
                case 't': {
                    string str_type(optarg);
                    if (str_type == "su") {
                        type = UNIFORM_SYNTHETIC;
                        rpath = r_synthetic_uniform_path;
                        lpath = l_synthetic_uniform_path;
                    } else if (str_type == "sz") {
                        type = ZIPF_SYNTHETIC;
                        rpath = r_synthetic_zipf_path;
                        lpath = l_synthetic_zipf_path;
                    } else if (str_type == "rd") {
                        type = ROVIO_TEST;
                        rpath = rovio_path;
                        lpath = rovio_path;
                    } else if (str_type == "sd") {
                        type = STOCK_TEST;
                        rpath = r_stock_path;
                        lpath = l_stock_path;
                    } else {
                        type = UNIFORM_SYNTHETIC;
                        rpath = r_synthetic_uniform_path;
                        lpath = l_synthetic_uniform_path;
                    }
                    break;
                }
                case 'm': {
                    string str_mode(optarg);
                    if (str_mode == "k") {
                        mode = KEY_BASED;
                    } else if (str_mode == "d") {
                        mode = DATA_BASED;
                    } else { mode = KEY_BASED; }
                    break;
                }
                case 'l': {
                    lower_bound = atoi(optarg);
                    break;
                }
                case 'u': {
                    upper_bound = atoi(optarg);
                    break;
                }
                case 'c': {
                    chaining = true;
                    break;
                }
                case 'o': {
                    outdir = string(optarg);
                    break;
                }
                default: {
                    cout << "Error in parsing the input arguments\n"
                            << endl;
                    exit(EXIT_FAILURE);
                }
            }
        }
    } else if (argc == 2 && ((option = getopt_long(argc, argv, "h", long_opts, &index)) != -1) && option == 'h') {
        cout << command_help << endl;

        cout
        << "Types:"
        << "\n\tsu = synthetic dataset with uniform distribution"
        << "\n\tsz = synthetic dataset with zipf distribution"
        << "\n\trd = rovio dataset"
        << "\n\tsd = stock dataset" << endl;

        exit(EXIT_SUCCESS);
    } else {
        cout << "Error in parsing the input arguments" << endl;
        cout << command_help << endl;

        exit(EXIT_FAILURE);
    }

    // data pre-processing
    vector<tuple_t> rdataset, ldataset;
    rdataset = parse_dataset(rpath, split_char);
    ldataset = parse_dataset(lpath, split_char);

    // display test configuration
    cout << app_descr << endl;
    if (rate != 0) {
        cout << "  * rate: " << rate << " tuples/second" << endl;
    }
    else {
        cout << "  * rate: full_speed tuples/second" << endl;
    }
    cout << "  * sampling: " << sampling << endl;
    cout << "  * batch size: " << batch_size << endl;
    cout << "  * lower bound: " << lower_bound << endl;
    cout << "  * upper bound: " << upper_bound << endl;
    if (type == UNIFORM_SYNTHETIC || type == ZIPF_SYNTHETIC) {
        cout << "  * data_size: " << data_size << endl;
        cout << "  * number of keys: " << num_keys << endl;
    }
    cout << "  * type: " << types_str[type] << endl;
    cout << "  * mode: " << modes_str[mode] << endl;
    cout << rsource_str << rsource_par_deg << endl;
    cout << lsource_str << lsource_par_deg << endl;
    cout << join_str << join_par_deg << endl;
    cout << sink_str << sink_par_deg << endl;

    cout
    << "  * TOPOLOGY\n"
    << "  * ==============================\n"
    << "  * rsource +--+ \n"
    << "  *            +--> join --> sink \n"
    << "  * lsource +--+ \n"
    << "  * ==============================\n";

    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    Execution_Mode_t exec_mode = Execution_Mode_t::DEFAULT;
    PipeGraph topology(topology_name, exec_mode, Time_Policy_t::EVENT_TIME);

    Source_Functor rsource_functor(rdataset, rate, app_start_time, batch_size, exec_mode);
    Source rsource = Source_Builder(rsource_functor)
                    .withParallelism(rsource_par_deg)
                    .withName(r_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();

    Source_Functor lsource_functor(ldataset, rate, app_start_time, batch_size, exec_mode);
    Source lsource = Source_Builder(lsource_functor)
                    .withParallelism(lsource_par_deg)
                    .withName(l_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();

    Interval_Join_Functor join_functor(app_start_time);
    Interval_Join_Builder join_build = Interval_Join_Builder(join_functor)
                            .withParallelism(join_par_deg)
                            .withName(join_name)
                            .withOutputBatchSize(batch_size)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withBoundaries(milliseconds(lower_bound), milliseconds(upper_bound));

    if (mode == test_mode::KEY_BASED) {
        join_build.withKPMode();
    } else {
        join_build.withDPSMode();
    }
    
    Interval_Join join = join_build.build();

    Sink_Functor sink_functor(sampling, app_start_time);
    Sink sink = Sink_Builder(sink_functor)
            .withParallelism(sink_par_deg)
            .withName(sink_name)
            .build();

    MultiPipe &r = topology.add_source(rsource);
    MultiPipe &l = topology.add_source(lsource);
    MultiPipe &join_pipe = r.merge(l);
    join_pipe.add(join);

    if (chaining) {
        cout << "  * chaining is enabled" << endl;
        join_pipe.chain_sink(sink);
    } else {
        cout << "  * chaining is disabled" << endl;
        join_pipe.add_sink(sink);
    }
    
    //cout << "Executing topology" << endl;
    cout << fixed << setprecision(2);

    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    
    double elapsed_time_seconds = static_cast<double>(end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    
    double mbs = ((total_bytes / 1048576) / elapsed_time_seconds);
    cout << "Measured throughput: " << (int) throughput << " tuples/second, " << mbs << " MB/s" << endl;
    
    //cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
#ifdef COLLECT_TEST_DATA
    dumpThroughput((int)throughput, (outdir +"throughput.json"));
#endif
    
    return 0;
}

vector<string> split(const string &target, const char delim)
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

vector<tuple_t> parse_dataset(const string &file_path, const char delim)
{
    vector<tuple_t> dataset;
    ifstream file(file_path);

    if (file.is_open())
    {
        string line;
        vector<string> tokens;

        // First line - Syntethic Parameters
        if (type == test_types::UNIFORM_SYNTHETIC ||type == test_types::ZIPF_SYNTHETIC)
        {
            getline(file, line);
            tokens = split(line, delim);
            if (tokens.size() != 2) {
                cout << "Error in parsing Syntethic parameters" << endl;
                exit(EXIT_FAILURE);
            }
            num_keys = stoul(tokens[0]);
            data_size = stoul(tokens[1]);
        }

        // Parsing tuples
        size_t key;
        int64_t value;
        uint64_t ts;
        while (getline(file, line))
        {
            if (!line.empty()) {
                tokens = split(line, delim);
                
                switch (type)
                {
                    case UNIFORM_SYNTHETIC:
                    case ZIPF_SYNTHETIC:
                        if (tokens.size() != 2) {
                            cout << "Error in parsing Syntethic tuple" << endl;
                            exit(EXIT_FAILURE);
                        }
                        key = stoul(tokens[0]);
                        ts = stoul(tokens[1]);
                        dataset.push_back(tuple_t(key, ts));
                        break;
                    case ROVIO_TEST:
                        if (tokens.size() != 4) {
                            cout << "Error in parsing tuple" << endl;
                            exit(EXIT_FAILURE);
                        }
                        key = stoul(tokens[0]);
                        value = stol(tokens[2]);
                        dataset.push_back(tuple_t(key, value));
                        break;
                    case STOCK_TEST:
                        if (tokens.size() != 2) {
                            cout << "Error in parsing tuple" << endl;
                            exit(EXIT_FAILURE);
                        }
                        key = stoul(tokens[0]);
                        value = stol(tokens[1]);
                        dataset.push_back(tuple_t(key, value));
                        break;
                }
            }
        }
        file.close();
    }
    return dataset;
}

void dumpThroughput(int newThroughput, const std::string& filename) {
    rapidjson::Document document;
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();

    std::ifstream ifs(filename);
    if (!ifs) {
        // File doesn't exist, create a new one with an array
        document.SetArray();
    } else {
        // File exists, read the existing data
        rapidjson::IStreamWrapper isw(ifs);
        document.ParseStream(isw);
        if (!document.IsArray()) {
            throw std::runtime_error("Existing file does not contain a JSON array");
        }
    }
    ifs.close();
    
    // Append new data and write it back to the file
    document.PushBack(rapidjson::Document().SetDouble(newThroughput), allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    document.Accept(writer);

    std::ofstream ofs(filename);
    if (!ofs.is_open()) {
        throw std::runtime_error("Could not open file for writing");
    }
    ofs << strbuf.GetString();
    ofs.close();
}
