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

#ifndef WORDCOUNT_SINK_HPP
#define WORDCOUNT_SINK_HPP

#include<algorithm>
#include<iomanip>
#include<ff/ff.hpp>
#include "../util/cli_util.hpp"
#include "../util/tuple.hpp"
#include "../util/sampler.hpp"
#include "../util/metric_group.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Sink_Functor class
class Sink_Functor
{
private:
    unsigned long app_start_time;
    unsigned long current_time;

    size_t parallelism;
    size_t replica_id;
    size_t joined;

    util::Sampler latency_sampler;
    long bytes_sum;
    long sampling;

public:
    // Constructor
    Sink_Functor(const long _sampling,
                 const unsigned long _app_start_time):
                 sampling(_sampling),
                 app_start_time(_app_start_time),
                 current_time(_app_start_time),
                 joined(0),
                 latency_sampler(_sampling) {}

    // operator() method
    void operator()(optional<tuple_t> &t, RuntimeContext &rc)
    {
        if (t) {
            if (joined == 0) {
                parallelism = rc.getParallelism();
                replica_id = rc.getReplicaIndex();
            }
            current_time = current_time_nsecs();
            unsigned long tuple_latency = (current_time - (*t).ts) / 1e06;
            joined++;// tuples counter
            latency_sampler.add(tuple_latency, current_time);
#if 1
            if ((*t).key != 10) {
                cout << "Received tuple: key-> " << (*t).key << ", value-> " << (*t).value << endl;
            }
#endif
        }
        else { // EOS
            long t_elapsed = (current_time - app_start_time) / 1e09;
            if (joined != 0) {
                cout << "[Sink] joined: "
                         << joined << " (tuples) "
                         << (bytes_sum / 1048576) << " (MB), "
                         << "bandwidth: "
                         << joined / t_elapsed << " (tuples/s) " << endl;
                util::metric_group.add("latency", latency_sampler);
            }
        }
    }
};

#endif //WORDCOUNT_SINK_HPP
