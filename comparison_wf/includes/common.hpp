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

#ifndef COMMON_HPP
#define COMMON_HPP

#include<vector>
#include<windflow.hpp>
#include"element.hpp"
#include "./util/cli_util.hpp"
#include "./util/tuple.hpp"
#include "./util/sampler.hpp"
#include "./util/metric_group.hpp"

using namespace wf;
using namespace std;

extern atomic<long> sent_tuples;

// Source_Functor class
class Source_Functor
{
private:
    const vector<Element> &dataset;
    size_t idx;
    long generated_tuples;
    uint64_t next_ts;

public:
    // Constructor
    Source_Functor(const vector<Element> &_dataset):
                   dataset(_dataset),
                   idx(0),
                   generated_tuples(0),
                   next_ts(0) {}

    // operator() method
    void operator()(Source_Shipper<Element> &shipper, RuntimeContext &rc)
    {
        for (int i=0; i<dataset.size(); i++) {
            Element el = dataset.at(idx);
            next_ts = el.ts;
            el.ts = current_time_nsecs();
            shipper.pushWithTimestamp(std::move(el), next_ts); // send the next tuple
            shipper.setNextWatermark(next_ts);
            generated_tuples++;
            idx++;
        }
        sent_tuples.fetch_add(generated_tuples);
    }
};

// Interval_Join class
class Interval_Join_Functor
{
public:
    optional<Element> operator()(const Element &baseEl, const Element &probeEl)
    {
        uint64_t ts = std::max(baseEl.ts, probeEl.ts);
        Element ris(baseEl.key, baseEl.val + probeEl.val, ts);
        return ris;
    }
};

// Sink_Functor class
class Sink_Functor
{
private:
    size_t joined;
    util::Sampler latency_sampler;
    unsigned long current_time;

public:
    // Constructor
    Sink_Functor(int _sampling_interval):
                 joined(0),
                 latency_sampler(_sampling_interval),
                 current_time(0) {}

    // operator() method
    void operator()(optional<Element> &el, RuntimeContext &rc)
    {
        if (el) {
            joined++;
            current_time = current_time_nsecs();
            unsigned long tuple_latency = (current_time - (*el).ts) / 1e03; // us precision
            latency_sampler.add(tuple_latency, current_time);
        }
        else {
            util::metric_group.add("latency", latency_sampler);
        }
    }
};

#endif
