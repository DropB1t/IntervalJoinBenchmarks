/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli and Alessandra Fais
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#ifndef IJ_SOURCE_HPP
#define IJ_SOURCE_HPP

#include<fstream>
#include<vector>
#include<ff/ff.hpp>
#include "../util/constants.hpp"
#include "../util/tuple.hpp" 

using namespace std;
using namespace ff;
using namespace wf;

extern atomic<long> sent_tuples;
extern atomic<long> total_bytes;

// Source_Functor class
class Source_Functor
{
private:
    Execution_Mode_t execution_mode;

    uint seed;
    uint64_t next_ts = 1704106800000000; // next timestamp in us starting from January 1, 2024 12:00:00 AM

    size_t tuple_size = sizeof(tuple_t);
    const vector<tuple_t> &dataset;

    size_t idx;
    size_t data_size;
    size_t batch_size;
    long generated_tuples;
    long generated_bytes;
    long nt_execution;

    unsigned long app_start_time;
    unsigned long current_time;
    int rate;

    // active_delay method
    void active_delay(unsigned long waste_time)
    {
        auto start_time = current_time_nsecs();
        bool end = false;
        while (!end) {
            auto end_time = current_time_nsecs();
            end = (end_time - start_time) >= waste_time;
        }
    }

public:
    // Constructor
    Source_Functor(const vector<tuple_t> &_dataset,
                   const int _rate,
                   const unsigned long _app_start_time,
                   const size_t _batch_size,
                   const uint _seed,
                   Execution_Mode_t _e):
                   dataset(_dataset),
                   rate(_rate),
                   idx(0),
                   generated_tuples(0),
                   generated_bytes(0),
                   nt_execution(0),
                   app_start_time(_app_start_time),
                   current_time(_app_start_time),
                   batch_size(_batch_size),
                   seed(_seed),
                   execution_mode(_e),
                   data_size(_dataset.size()) {}

    // operator() method
    void operator()(Source_Shipper<tuple_t> &shipper)
    {
        std::uniform_int_distribution<int> distribution(300, 750);
        std::mt19937 generator;
        generator.seed(seed);

        current_time = current_time_nsecs(); // get the current time
        while ( /* idx < data_size */ current_time - app_start_time <= app_run_time) // generation loop
        {
            /* if ((batch_size > 0) && (generated_tuples % batch_size == 0)) {
                current_time = current_time_nsecs(); // get the new current time
            }
            if (batch_size == 0) {
                current_time = current_time_nsecs(); // get the new current time
            } */

            tuple_t t = dataset.at(idx);
            generated_bytes += tuple_size;
            generated_tuples++;

            t.ts = current_time_nsecs();
            shipper.pushWithTimestamp(std::move(t), next_ts); // send the next tuple
            if (execution_mode == Execution_Mode_t::DEFAULT) {
                shipper.setNextWatermark(next_ts);
            }

            idx++;
            if (idx >= data_size) { // check the dataset boundaries
                idx = 0;
                nt_execution++;
            }

            if (rate != 0) { // active waiting to respect the generation rate
                long delay_nsec = (long) ((1.0 / rate) * 1e9);
                active_delay(delay_nsec);
            }

            auto offset = (distribution(generator)+1);
            next_ts += offset*1000;

            current_time = current_time_nsecs();
        }
        sent_tuples.fetch_add(generated_tuples);
        total_bytes.fetch_add(generated_bytes);
    }
};

#endif //IJ_SOURCE_HPP
