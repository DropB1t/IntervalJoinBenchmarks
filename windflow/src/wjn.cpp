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

#include<string>
#include<vector>
#include<iostream>
#include<chrono>
#include<unordered_map>
#include<cassert>
#include<algorithm>
#include<fstream>
#include<sstream>
#include<cmath>
#include<random>

#include<windflow.hpp>
#include<ff/ff.hpp>
#include "nodes/source.hpp"
#include "nodes/join.hpp"
#include "nodes/sink.hpp"
#include "util/props.hpp"
#include "util/cli_util_2.hpp"
#include "util/tuple.hpp"
#include "util/util.hpp"

using namespace std;
using namespace chrono;
using namespace ff;
using namespace wf;

typedef enum
{
    SYNTHETIC,
    ROVIO_TEST,
    STOCK_TEST
} test_types;

typedef enum
{
    KEY_BASED,
    DATA_BASED,
    HYBRID_BASED
} test_mode;

atomic<long> sent_tuples; // total number of tuples sent by all the sources
atomic<long> total_bytes; // total number of bytes processed by the system
string outdir="";
test_types type; // type of the dataset
int num_keys; // total number of keys in the dataset
int num_tuples; // total number of tuples in the dataset

vector<tuple_t> parse_dataset(const string &file_path,
                              const char delim,
                              int *n_keys,
                              int *size)
{
    vector<tuple_t> dataset;
    ifstream file(file_path);
    if (file.is_open()) {
        string line;
        vector<string> tokens;
        // First line - Syntethic Parameters
        if (type == test_types::SYNTHETIC) {
            getline(file, line);
            tokens = split(line, delim);
            if (tokens.size() != 2) {
                cout << "Error in parsing syntethic parameters" << endl;
                exit(EXIT_FAILURE);
            }
            *n_keys = stoul(tokens[0]);
            *size = stoul(tokens[1]);
        }
        // Parsing tuples
        size_t key;
        int64_t value;
        uint64_t ts = 0;
        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<std::mt19937::result_type> val_dist(1,100);
        while (getline(file, line)) {
            if (!line.empty()) {
                tokens = split(line, delim);
                switch (type) {
                    case SYNTHETIC:
                        if (tokens.size() != 2) {
                            cout << "Error in parsing Syntethic tuple" << endl;
                            exit(EXIT_FAILURE);
                        }
                        key = stoul(tokens[0]) - 1;
                        ts = stoul(tokens[1]);
                        value = val_dist(rng);
                        dataset.push_back(tuple_t(key, value, ts));
                        break;
                    case ROVIO_TEST:
                        abort(); // does not work at the moment
                        if (tokens.size() != 4) {
                            cout << "Error in parsing tuple" << endl;
                            exit(EXIT_FAILURE);
                        }
                        key = stoul(tokens[0]) - 1;
                        value = stol(tokens[2]);
                        dataset.push_back(tuple_t(key, value, ts));
                        break;
                    case STOCK_TEST:
                        abort(); // does not work at the moment
                        if (tokens.size() != 2) {
                            cout << "Error in parsing tuple" << endl;
                            exit(EXIT_FAILURE);
                        }
                        key = stoul(tokens[0]) - 1;
                        value = stol(tokens[1]);
                        dataset.push_back(tuple_t(key, value, ts));
                        break;
                }
            }
        }
        file.close();
    }
    return dataset;
}

// struct key descriptor
struct Key_d
{
    int key = 0;
    double prob = 0;
};

// struct representing a Joiner (i.e., Interval_Join replica)
struct Joiner
{
    std::vector<int> assigned_keys;
    double load = 0.0;
};

// function to check the actual balanceness
double balancing(const std::vector<Joiner> &joiners)
{
    double totalLoad = 0.0;
    double maxLoad = 0.0;
    for (const auto &j: joiners) {
        totalLoad += j.load;
        if (j.load > maxLoad) {
            maxLoad = j.load;
        }
    }
    double averageLoad = totalLoad / joiners.size();
    return (maxLoad / averageLoad);
}

// function to check that all joiners have a positive load
bool allLoaded(const std::vector<Joiner> &joiners)
{
    bool loaded = true;
    for (auto &j: joiners) {
        if (j.load == 0) {
            loaded = false;
        }
    }
    return loaded;
}

// check that for all keys the max splitting degree has been reached
bool allMaxSplit(std::vector<int> splits,
                 int max_splitting)
{
    for (auto &d: splits) {
        if (d < max_splitting) {
            return false;
        }
    }
    return true;
}

// function to compute the probabilities of the key
void compute_probabilities(std::vector<tuple_t> &dataset,
                           std::vector<double> &probs)
{
    std::vector<int> counters(num_keys, 0);
    for (tuple_t &t: dataset) {
        counters[t.key]++;
    }
    for (int &c: counters) {
        double p = ((double) c) / num_tuples;
        probs.push_back(p);
    }
}

// function to print the assignment
void printAssignment(std::vector<Joiner> &joiners)
{
    std::cout << "Assignment Joiners -> Keys" << std::endl;
    int idx = 0;
    for (auto &j: joiners) {
        std::cout << "Joiner " << idx++ << " has load " << j.load << ", keys { ";
        for (auto k: j.assigned_keys) {
            std::cout << k << ", ";
        }
        std::cout << "}" << std::endl;
    }
}

// compute average splitting degree
double averageSplitting(std::vector<int> splits)
{
    double res = 0;
    for (auto &s: splits) {
        res += s;
    }
    return (res / splits.size());
}

// function to assign keys to joiners
void assignKeys(int numJoiners,
                double threshold,
                std::vector<double> &probs,
                std::unordered_map<int, std::vector<int>> &keyToJoiners,
                std::vector<Joiner> &joiners,
                size_t max_splitting)
{
    assert(probs.size() == num_keys); // sanity check
    assert(joiners.size() == numJoiners); // sanity check
    std::vector<int> splitDegrees;
    splitDegrees.assign(num_keys, 0);
    std::vector<Key_d> sortedKeys;
    for (int i=0; i<num_keys; i++) {
        sortedKeys.push_back({i, probs[i]});
    }
    // sort keys in descending order or probabilities
    std::sort(sortedKeys.begin(), sortedKeys.end(), [](const Key_d k1, const Key_d k2) {
        return k1.prob > k2.prob;
    });
    // FIRST PASS: assign each key to a single Joiner
    for (const auto &key_d: sortedKeys) {
        // find the Joiner with the minimum load
        auto minJoiner = std::min_element(joiners.begin(), joiners.end(), [](Joiner j1, Joiner j2) {
            return j1.load < j2.load;
        });
        // assign the key to this joiner
        (minJoiner->assigned_keys).push_back(key_d.key);
        // update the load of this joiner
        minJoiner->load += key_d.prob;
        // update the splitting degree of the key
        splitDegrees[key_d.key] = 1;
        // update the key to joiner map
        keyToJoiners[key_d.key].push_back(minJoiner - joiners.begin());
    }
    // Check if the load is sufficiently balanced
    if ((balancing(joiners) <= threshold) && allLoaded(joiners)) {
        std::cout << "STOP FIRST PASS -> final balancing is: " << balancing(joiners) << ", threshold was: " << threshold << ", average splitting degree: " << averageSplitting(splitDegrees) << std::endl;
        return;
    }
    // SECOND PASS: reassign keys to multiple joiners if needed
    bool balanced = false;
    while (!balanced) {
        for (const auto &key_d: sortedKeys) {
            if (splitDegrees[key_d.key] < max_splitting) {
                // find the joiner with the minimum load that does not already have this key
                auto minJoiner = std::min_element(joiners.begin(), joiners.end(), [&key_d, &keyToJoiners, &joiners](const Joiner &j1, const Joiner &j2) {
                    auto &vec = keyToJoiners[key_d.key];
                    bool j1HasKey = std::find(vec.begin(), vec.end(), &j1 - &joiners[0]) != vec.end();
                    bool j2HasKey = std::find(vec.begin(), vec.end(), &j2 - &joiners[0]) != vec.end();
                    if (j1HasKey && !j2HasKey) return false;
                    if (!j1HasKey && j2HasKey) return true;
                    return j1.load < j2.load;
                });
                // ensure the selected worker does not already have the key
                if (std::find(keyToJoiners[key_d.key].begin(), keyToJoiners[key_d.key].end(), minJoiner - joiners.begin()) == keyToJoiners[key_d.key].end()) {
                    for (auto &j: joiners) {
                        if (std::find((j.assigned_keys).begin(), (j.assigned_keys).end(), key_d.key) != (j.assigned_keys).end()) {
                            j.load = j.load - (key_d.prob / splitDegrees[key_d.key]) + (key_d.prob / (splitDegrees[key_d.key] + 1));
                        }
                    }
                    // assign the key to this joiner
                    (minJoiner->assigned_keys).push_back(key_d.key);
                    // update the load of this joiner
                    minJoiner->load += key_d.prob / (splitDegrees[key_d.key] + 1);
                    // update the splitting degree of the key
                    splitDegrees[key_d.key]++;
                    // update the key to joiner map
                    keyToJoiners[key_d.key].push_back(minJoiner - joiners.begin());
                    if ((balancing(joiners) <= threshold) && allLoaded(joiners)) {
                        balanced = true;
                        break;
                    }
                }
                else {
                    abort(); // max_splitting has been reached, absurd!
                }
            }
        }
        // check if the load is now sufficiently balanced
        if (((balancing(joiners) <= threshold) && allLoaded(joiners)) || allMaxSplit(splitDegrees, max_splitting)) {
            balanced = true;
        }
    }
    for (Joiner &j: joiners) {
        std::sort((j.assigned_keys).begin(), (j.assigned_keys).end(), [](int k1, int k2) {
            return k1 < k2;
        });
    }
    std::cout << "STOP SECOND PASS -> final balancing is: " << balancing(joiners) << ", threshold was: " << threshold << ", average splitting degree: " << averageSplitting(splitDegrees) << std::endl;
    return;
}

int main(int argc, char *argv[])
{
    // parse arguments from command line
    int option = 0;
    int index = 0;
    string rpath;
    string lpath;
    test_mode mode;
    size_t rsource_par_deg = 0;
    size_t lsource_par_deg = 0;
    size_t join_par_deg = 0;
    size_t sink_par_deg = 0;
    size_t hybrid_par_deg = 0;
    int64_t window_len = 0;
    int64_t slide_len = 0;
    sent_tuples = 0;
    total_bytes = 0;
    size_t batch_size = 0;
    bool chaining = false;
    long sampling = 250;
    int rate = 0;
    double threshold = 1.2;
    if (argc >= 13 && argc <= 20) {
        while ((option = getopt_long(argc, argv, "b:p:t:m:w:s:c:o:h:e:", long_opts, &index)) != -1) {
            switch (option) {
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
                        cout << "Error in parsing the input arguments\n" << endl;
                        exit(EXIT_FAILURE);
                    }
                    else {
                        rsource_par_deg = par_degs[0];
                        lsource_par_deg = par_degs[1];
                        join_par_deg = par_degs[2];
                        sink_par_deg = par_degs[3];
                    }
                    break;
                }
                case 't': {
                    // TODO: erroneous type parsing hence for stock tests we have different datasets for base and probe
                    string str_type(optarg);
                    rpath = str_type;
                    lpath = rpath;
                    std::string to_replace = "r_";
                    std::string replacement = "l_";
                    size_t pos = lpath.find(to_replace);
                    if (pos != std::string::npos) {
                        lpath.replace(pos, to_replace.length(), replacement);
                    }
                    break;
                }
                case 'm': {
                    string str_mode(optarg);
                    if (str_mode == "k") {
                        mode = KEY_BASED;
                    }
                    else if (str_mode == "d") {
                        mode = DATA_BASED;
                    }
                    else if (str_mode == "h") {
                        mode = HYBRID_BASED;
                    }
                    else {
                        abort();
                    }
                    break;
                }
                case 'w': {
                    window_len = atoi(optarg);
                    break;
                }
                case 's': {
                    slide_len = atoi(optarg);
                    break;
                }
                case 'c': {
                    chaining = true;
                    break;
                }
                case 'o': {
                    outdir = string(optarg) + "/";
                    break;
                }
                case 'h': {
                    hybrid_par_deg = atoi(optarg);
                    break;
                }
                case 'e': {
                    threshold = atof(optarg);
                    break;
                }
                default: {
                    cout << "Error in parsing the input arguments\n" << endl;
                    exit(EXIT_FAILURE);
                }
            }
        }
    }
    else if (argc == 2 && ((option = getopt_long(argc, argv, "h", long_opts, &index)) != -1) && option == 'h') {
        cout << command_help_2 << endl;
        exit(EXIT_SUCCESS);
    }
    else {
        cout << "Error in parsing the input arguments" << endl;
        cout << command_help_2 << endl;
        exit(EXIT_FAILURE);
    }
    // data pre-processing
    int size1, size2;
    int n_keys1, n_keys2;
    vector<tuple_t> rdataset, ldataset;
    rdataset = parse_dataset(rpath, split_char, &n_keys1, &size1);
    ldataset = parse_dataset(lpath, split_char, &n_keys2, &size2);
    assert(n_keys1 == n_keys2);
    num_keys = n_keys1;
    num_tuples = size1 + size2;
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
    cout << "  * window length: " << window_len << endl;
    cout << "  * slide length: " << slide_len << endl;
    cout << "  * number of keys: " << num_keys << endl;
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
    Source_Functor rsource_functor(rdataset, rate, app_start_time, batch_size, exec_mode, true);
    Source rsource = Source_Builder(rsource_functor)
                    .withParallelism(rsource_par_deg)
                    .withName(r_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();
    Source_Functor lsource_functor(ldataset, rate, app_start_time, batch_size, exec_mode, true);
    Source lsource = Source_Builder(lsource_functor)
                    .withParallelism(lsource_par_deg)
                    .withName(l_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();
    Join_Functor join_functor(app_start_time);
    Window_Join_Builder join_build = Window_Join_Builder(join_functor)
    									.withParallelism(join_par_deg)
			                            .withName(join_name)
			                            .withOutputBatchSize(batch_size)
			                            .withKeyBy([](const tuple_t &t) -> int { return t.key; })
			                            .withSlidingWindows(milliseconds(window_len), milliseconds(slide_len));
    if (mode == test_mode::KEY_BASED) {
        join_build.withKPMode();
    }
    else if (mode == test_mode::DATA_BASED) {
        join_build.withDPMode();
    }
    else if (mode == test_mode::HYBRID_BASED) {
        if (hybrid_par_deg == 0) {
            std::cout << "hybrid mode requires a positive parallelism expressed with -h <value>" << std::endl;
            exit(1);
        }
        std::vector<tuple_t> dataset = rdataset;
        dataset.insert(dataset.end(), ldataset.begin(), ldataset.end());
        std::vector<double> probs;
        compute_probabilities(dataset, probs);
        std::unordered_map<int, std::vector<int>> keyToJoiners;
        std::vector<Joiner> joiners(join_par_deg);
        assignKeys(join_par_deg,
                   threshold,
                   probs,
                   keyToJoiners,
                   joiners,
                   hybrid_par_deg);
        printAssignment(joiners);
        join_build.withHPMode(hybrid_par_deg, keyToJoiners);
    }
    else {
        abort();
    }
    Window_Join join = join_build.build();
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
    }
    else {
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
    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
#ifdef METRICS_COLLECTION
        rapidjson::Document doc;
        doc.SetInt(throughput);
        dump_test_results(doc, (outdir +"throughput.json"));
#endif
    return 0;
}
