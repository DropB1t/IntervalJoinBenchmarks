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
#include<windflow.hpp>
#include"../includes/element.hpp"
#include"../includes/common.hpp"

using namespace std;
using namespace chrono;
using namespace wf;

#define PROBE_LEN 2000000
#define BASE_LEN 1000000

atomic<long> sent_tuples; // total number of tuples sent by all the sources

// Method to generate synthetic streams
vector<Element> generate_stream(int count,
                                int key_num,
                                uint64_t max_ts)
{
    if (max_ts < count - 1) {
        max_ts = count - 1;
    }
    vector<Element> stream;
    stream.resize(count);
    uint64_t step = max_ts / count;
    if (step < 1) {
        step = 1;
    }
    int64_t count_per_key = count / key_num;
    if (count_per_key * key_num != count) {
        std::cerr << "Element count is not divided by key_num!" << std::endl;
        exit(1);
    }
    for (int i = 0; i < count_per_key; i++) {
        for (int k = 0; k < key_num; k++) {
            int idx = i * key_num + k;
            uint64_t ts = 0;
            ts = (i * key_num + k) * step;
            stream.push_back(Element(std::string("key_") + std::to_string(k), std::to_string(ts), ts));
        }
    }
    return stream;
}

// struct key descriptor
struct Key_d
{
    std::string key;
    double prob = 0;
};

// struct representing a Joiner (i.e., Interval_Join replica)
struct Joiner
{
    std::vector<std::string> assigned_keys;
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

// extract the key ID as an integer
int extractKeyID(const std::string &input)
{
    std::string prefix = "key_";
    size_t pos = input.find(prefix);
    if (pos != std::string::npos) {
        std::string numberStr = input.substr(pos + prefix.length());
        return std::stoi(numberStr);
    }
    else {
        return -1;
    }
}

// function to assign keys to joiners
void assignKeys(int numJoiners,
                double threshold,
                std::vector<double> &probs,
                std::unordered_map<std::string, std::vector<int>> &keyToJoiners,
                std::vector<Joiner> &joiners,
                size_t max_splitting,
                int num_keys)
{
    assert(probs.size() == num_keys); // sanity check
    assert(joiners.size() == numJoiners); // sanity check
    std::vector<int> splitDegrees;
    splitDegrees.assign(num_keys, 0);
    std::vector<Key_d> sortedKeys;
    for (int i=0; i<num_keys; i++) {
        sortedKeys.push_back({"key_" + std::to_string(i), probs[i]});
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
        splitDegrees[extractKeyID(key_d.key)] = 1;
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
            if (splitDegrees[extractKeyID(key_d.key)] < max_splitting) {
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
                            j.load = j.load - (key_d.prob / splitDegrees[extractKeyID(key_d.key)]) + (key_d.prob / (splitDegrees[extractKeyID(key_d.key)] + 1));
                        }
                    }
                    // assign the key to this joiner
                    (minJoiner->assigned_keys).push_back(key_d.key);
                    // update the load of this joiner
                    minJoiner->load += key_d.prob / (splitDegrees[extractKeyID(key_d.key)] + 1);
                    // update the splitting degree of the key
                    splitDegrees[extractKeyID(key_d.key)]++;
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
        std::sort((j.assigned_keys).begin(), (j.assigned_keys).end(), [](std::string k1, std::string k2) {
            return k1 < k2;
        });
    }
    std::cout << "STOP SECOND PASS -> final balancing is: " << balancing(joiners) << ", threshold was: " << threshold << ", average splitting degree: " << averageSplitting(splitDegrees) << std::endl;
    return;
}

// main
int main(int argc, char *argv[])
{
    int option = 0;
    size_t base_source_par_deg = 0;
    size_t probe_source_par_deg = 0;
    size_t join_par_deg = 0;
    size_t sink_par_deg = 0;
    sent_tuples = 0;
    size_t batch_size = 0;
    bool chaining = false;
    int num_keys = 0;
    int win_len = 0;
    int sampling_interval = 5000;
    if (argc == 10 || argc == 9) {
        while ((option = getopt(argc, argv, "b:p:k:w:c")) != -1) {
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
                        base_source_par_deg = par_degs[0];
                        probe_source_par_deg = par_degs[1];
                        join_par_deg = par_degs[2];
                        sink_par_deg = par_degs[3];
                    }
                    break;
                }
                case 'k': {
                    num_keys = atoi(optarg);
                    break;
                }
                case 'w': {
                    win_len = atoi(optarg);
                    break;
                }
                case 'c': {
                    chaining = true;
                    break;
                }
                default: {
                    cout << "Error in parsing the input arguments\n" << endl;
                    exit(EXIT_FAILURE);
                }
            }
        }
    }
    else {
        cout << "Error in parsing the input arguments" << endl;
        cout << "test_comparison -b <batch_size> -p <nRSource,nLSource,nJoin,nSink> -k <num_keys> -w <win_len_usec> [-c]" << std::endl;
        exit(EXIT_FAILURE);
    }

    // prepare the streams
    uint64_t max_ts = std::max(BASE_LEN, PROBE_LEN);
    vector<Element> probe_stream = generate_stream(PROBE_LEN, num_keys, max_ts);
    vector<Element> base_stream = generate_stream(BASE_LEN, num_keys, max_ts);

    // display test configuration
    cout << "Test Interval_Join (WindFlow) prepares to run..." << endl;

    // application starting time
    unsigned long app_start_time = current_time_nsecs();
    PipeGraph topology("comparison_wf", Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);

    Source_Functor base_source_functor(base_stream);
    Source base_source = Source_Builder(base_source_functor)
                    .withParallelism(base_source_par_deg)
                    .withName("Base Source")
                    .withOutputBatchSize(batch_size)
                    .build();

    Source_Functor probe_source_functor(probe_stream);
    Source probe_source = Source_Builder(probe_source_functor)
                    .withParallelism(probe_source_par_deg)
                    .withName("Probe Stream")
                    .withOutputBatchSize(batch_size)
                    .build();

    Interval_Join_Functor join_functor;
    std::vector<double> probs(num_keys, 1.0/(num_keys));
    std::unordered_map<std::string, std::vector<int>> keyToJoiners;
    std::vector<Joiner> joiners(join_par_deg);
    assignKeys(join_par_deg, 1.2, probs, keyToJoiners, joiners, join_par_deg, num_keys);
    printAssignment(joiners);
    Interval_Join_Builder join_build = Interval_Join_Builder(join_functor)
                            .withParallelism(join_par_deg)
                            .withName("OIJ")
                            .withOutputBatchSize(batch_size)
                            .withKeyBy([](const Element &e) -> std::string { return e.key; })
                            .withBoundaries(microseconds(win_len * (-1)), microseconds(0))
                            .withHPMode(join_par_deg, keyToJoiners);
    Interval_Join join = join_build.build();

    Sink_Functor sink_functor(sampling_interval);
    Sink sink = Sink_Builder(sink_functor)
                .withParallelism(sink_par_deg)
                .withName("Sink")
                .build();

    MultiPipe &base_mp = topology.add_source(base_source);
    MultiPipe &probe_mp = topology.add_source(probe_source);
    MultiPipe &join_mp = base_mp.merge(probe_mp);
    join_mp.add(join);
    if (chaining) {
        cout << "  * chaining is enabled" << endl;
        join_mp.chain_sink(sink);
    }
    else {
        cout << "  * chaining is disabled" << endl;
        join_mp.add_sink(sink);
    }
    // evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    double elapsed_time_seconds = static_cast<double>(end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    cout << "Measured throughput: " << (int) throughput << " tuples/second" << endl;
    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    cout << "...end" << endl;
    return 0;
}
