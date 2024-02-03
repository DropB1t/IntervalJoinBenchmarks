
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
                int64_t value = stol((type == test_types::STOCK_TEST ? tokens.at(0) : tokens.at(2)));

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
#if 0
            if (i < 10) {
                cout << "Generated tuple: key-> " << t.key << ", value-> " << t.value << endl;
            }
#endif
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

    size_t rsource_par_deg = 0;
    size_t lsource_par_deg = 0;
    size_t join_par_deg = 0;
    size_t sink_par_deg = 0;

    size_t lower_bound = 0;
    size_t upper_bound = 0;

    sent_tuples = 0;
    total_bytes = 0;

    bool chaining = false;
    long sampling = 0;
    int rate = 0;

    size_t batch_size = 0;
    size_t num_keys = 0;

    if (argc == 17 || argc == 18) {
        while ((option = getopt_long(argc, argv, "r:k:s:b:p:t:l:u:c:", long_opts, &index)) != -1) {
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
    if (type == UNIFORM_SYNTHETIC || type == ZIPF_SYNTHETIC) {
        cout << "  * data_size: " << data_size << endl;
        cout << "  * number of keys: " << num_keys << endl;
    }
    cout << "  * type: " << types_str[type] << endl;
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

    PipeGraph topology(topology_name, Execution_Mode_t::DEFAULT, Time_Policy_t::EVENT_TIME);
    Source_Functor rsource_functor(rdataset, rate, app_start_time, batch_size, rseed);
    Source rsource = Source_Builder(rsource_functor)
                    .withParallelism(rsource_par_deg)
                    .withName(r_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();

    Source_Functor lsource_functor(ldataset, rate, app_start_time, batch_size, lseed);
    Source lsource = Source_Builder(lsource_functor)
                    .withParallelism(lsource_par_deg)
                    .withName(l_source_name)
                    .withOutputBatchSize(batch_size)
                    .build();

    Interval_Join_Functor join_functor(app_start_time);
    Interval_Join join = Interval_Join_Builder(join_functor)
                            .withParallelism(join_par_deg)
                            .withName(join_name)
                            .withOutputBatchSize(batch_size)
                            .withKeyBy([](const tuple_t &t) -> size_t { return t.key; })
                            .withBoundaries(microseconds(lower_bound), microseconds(upper_bound))
                            .withKPMode()
                            .build();

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
        cout << "Chaining is enabled" << endl;
        join_pipe.chain_sink(sink);
    } else {
        cout << "Chaining is disabled" << endl;
        join_pipe.add_sink(sink);
    }

    cout << "Executing topology" << endl;
    /// evaluate topology execution time
    volatile unsigned long start_time_main_usecs = current_time_usecs();
    topology.run();
    volatile unsigned long end_time_main_usecs = current_time_usecs();
    cout << "Exiting" << endl;
    double elapsed_time_seconds = (end_time_main_usecs - start_time_main_usecs) / (1000000.0);
    double throughput = sent_tuples / elapsed_time_seconds;
    double mbs = (double)((total_bytes / 1048576) / elapsed_time_seconds);
    cout << "Measured throughput: " << (int) throughput << " tuples/second, " << mbs << " MB/s" << endl;
    cout << "Dumping metrics" << endl;
    util::metric_group.dump_all();
    

    return 0;
}