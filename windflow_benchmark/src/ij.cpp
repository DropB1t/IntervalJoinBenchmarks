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

#include "../includes/nodes/source.hpp"
#include "../includes/nodes/join.hpp"
#include "../includes/nodes/sink.hpp"

#include "util/constants.hpp"
#include "util/cli_util.hpp"
#include "util/zipfian_int_distribution.h"
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

vector<tuple_t> parse_dataset(const string &file_path, const char delim, test_types type)
{
    vector<tuple_t> dataset;
    ifstream file(file_path);
    if (file.is_open())
    {
        string line;
        while (getline(file, line))
        {
            if (!line.empty())
            {
                vector<string> tokens = split(line, delim);

                size_t key = stoul(tokens.at(0));
                int64_t value = stol((type == test_types::STOCK_TEST ? tokens.at(1) : tokens.at(2)));

                dataset.push_back(tuple_t(key, value));
            }
        }
        file.close();
    }
    return dataset;
}

vector<tuple_t> create_tuples_uniform_keys(int num_keys, int size, uint seed)
{
    vector<tuple_t> dataset;

    mt19937 rng;
    rng.seed(seed);

    auto dist = uniform_int_distribution<int>(1, num_keys);

    for (int i = 0; i < size; i++)
    {
        tuple_t t(dist(rng));
        dataset.push_back(t);
    }
    return dataset;
}

vector<tuple_t> create_tuples_zipf_keys(int num_keys, int size, uint seed)
{
    vector<tuple_t> dataset;

    mt19937 rng;
    rng.seed(seed);

    auto dist = zipfian_int_distribution<int>(1, num_keys, zipf_exponent);

    for (int i = 0; i < size; i++)
    {
        tuple_t t(dist(rng));
        dataset.push_back(t);
    }
    return dataset;
}

int main(int argc, char *argv[])
{
    /// parse arguments from command line
    int option = 0;
    int index = 0;

    string file_path;
    test_types type;
    test_mode mode;

    size_t rsource_par_deg = 0;
    size_t lsource_par_deg = 0;
    size_t join_par_deg = 0;
    size_t sink_par_deg = 0;

    int64_t lower_bound = 0;
    int64_t upper_bound = 0;

    sent_tuples = 0;
    total_bytes = 0;

    bool chaining = false;
    long sampling = 0;
    int rate = 0;

    size_t batch_size = 0;
    size_t num_keys = 0;

    if (argc == 19 || argc == 20) {
        while ((option = getopt_long(argc, argv, "r:k:s:b:p:t:m:l:u:c:", long_opts, &index)) != -1) {
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
                case 'k': {
                    num_keys = atoi(optarg);
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
                    } else if (str_type == "sz") {
                        type = ZIPF_SYNTHETIC;
                    } else if (str_type == "rd") {
                        type = ROVIO_TEST;
                    } else if (str_type == "sd") {
                        type = STOCK_TEST;
                    } else { type = UNIFORM_SYNTHETIC; }
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

    // data pre-processing
    vector<tuple_t> rdataset, ldataset;

    switch (type)
    {
        case ZIPF_SYNTHETIC:
            rdataset = create_tuples_zipf_keys(num_keys, data_size, rseed);
            ldataset = create_tuples_zipf_keys(num_keys, data_size, lseed);
            break;
        case ROVIO_TEST:
            rdataset = parse_dataset(rovio_path, '|', ROVIO_TEST);
            ldataset = parse_dataset(rovio_path, '|', ROVIO_TEST);
            break;
        case STOCK_TEST:
            rdataset = parse_dataset(r_stock_path, '|', STOCK_TEST);
            ldataset = parse_dataset(l_stock_path, '|', STOCK_TEST);
            break;
        case UNIFORM_SYNTHETIC:    
        default:
            rdataset = create_tuples_uniform_keys(num_keys, data_size, rseed);
            ldataset = create_tuples_uniform_keys(num_keys, data_size, lseed);
            break;
    }

    /// application starting time
    unsigned long app_start_time = current_time_nsecs();
    Execution_Mode_t exec_mode = Execution_Mode_t::DEFAULT;

    PipeGraph topology(topology_name, exec_mode, Time_Policy_t::EVENT_TIME);
    Source_Functor rsource_functor(rdataset, rate, app_start_time, batch_size, rseed, exec_mode);
    Source rsource = Source_Builder(rsource_functor)
                    .withParallelism(rsource_par_deg)
                    .withName(r_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();

    Source_Functor lsource_functor(ldataset, rate, app_start_time, batch_size, lseed, exec_mode);
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
    
    cout << fixed << setprecision(2);
    cout << "Executing topology" << endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    double elapsed_time_seconds = static_cast<double>(end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    double mbs = ((total_bytes / 1048576) / elapsed_time_seconds);
    cout << "Measured throughput: " << (int) throughput << " tuples/second, " << mbs << " MB/s" << endl;
    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    

    return 0;
}