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

#ifndef WORDCOUNT_COUNTER_HPP
#define WORDCOUNT_COUNTER_HPP

#include<ff/ff.hpp>
#include<string>
#include<vector>
#include<regex>
#include "../util/tuple.hpp"
#include "../util/constants.hpp"
#include "../util/cli_util.hpp"

using namespace std;
using namespace ff;
using namespace wf;

// Interval_Join class
class Interval_Join_Functor
{
private:
    unsigned long app_start_time;
    unsigned long current_time;
    size_t parallelism;
    size_t replica_id;
    size_t processed;

public:
    // Constructor
    Interval_Join_Functor(const unsigned long _app_start_time):
                    processed(0),
                    app_start_time(_app_start_time),
                    current_time(_app_start_time) {} 

    optional<tuple_t> operator()(const tuple_t &a, const tuple_t &b, RuntimeContext &rc)
    {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }

        tuple_t out(a.key, (a.value + b.value));
        out.ts = max(a.ts, b.ts);

        processed++;
        current_time = current_time_nsecs();

        return out;
    }

    // Destructor
    ~Interval_Join_Functor()
    {
        if (processed != 0) {
            cout << "[Interval_Join] replica " << replica_id + 1 << "/" << parallelism
                 << ", execution time: " << (current_time - app_start_time) / 1e09
                 << " s, joined: " << processed << " tuples"
                 << ", bandwidth: " << processed / ((current_time - app_start_time) / 1e09)
                 << " (tuples/s) " << endl;
        }
    }
};

#endif //WORDCOUNT_COUNTER_HPP
