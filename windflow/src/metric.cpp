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

#include "util/props.hpp"
#include "util/util.hpp"
#include "util/metric.h"

#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <fstream>
#include <numeric>
#include <sstream>
#include <iostream>

extern string outdir;

using namespace rapidjson;

namespace util {

Metric::Metric(const std::string &name)
    : name_(name)
{}

void Metric::add(double value)
{
    samples_.push_back(value);
}

void Metric::total(long total)
{
    total_ += total;
}

/**
 * Dumps the metric data to a JSON file.
 * If METRICS_COLLECTION is defined, the metric data is dumped to a file with a specific name in the specified output directory.
 * Otherwise, the metric data is dumped to a file with a generic name.
 */
void Metric::dump()
{
    Document doc = this->get_json();
#ifdef METRICS_COLLECTION
    dump_test_results(doc, outdir + name_ + ".json");
#else
    string filename = "metric_" + name_ + ".json";
    dump_json(doc, filename);
#endif

#ifdef JOIN_ALL_LATENCIES
    std::ofstream file("all_latencies.csv");
    if (file.is_open()) {
        for (size_t i = 0; i < samples_.size(); ++i) {
            file << samples_[i];
            if (i < samples_.size() - 1) {
                file << ",";
            }
        }
        file << "\n";
        file.close();
    }
#endif
}

rapidjson::Document Metric::get_json()
{
    StringBuffer buffer;
    PrettyWriter<StringBuffer> writer(buffer);

    writer.StartObject();

    //writer.Key("name");
    //writer.String(name_.c_str());

    writer.Key("total");
    writer.Uint(total_);

    writer.Key("samples");
    writer.Uint(samples_.size());

    writer.Key("mean");
    writer.Double(std::accumulate(samples_.begin(), samples_.end(), 0.0) / samples_.size());

    auto minmax = std::minmax_element(samples_.begin(), samples_.end());
    double min = *minmax.first;
    double max = *minmax.second;

    writer.Key("0");
    writer.Double(min);

    // XXX no interpolation since we are dealing with *many* samples

    // add percentiles
    for (auto percentile : {0.05, 0.25, 0.5, 0.75, 0.95}) {
        auto pointer = samples_.begin() + samples_.size() * percentile;
        std::nth_element(samples_.begin(), pointer, samples_.end());
        auto label = std::to_string(int(percentile * 100));
        auto value = *pointer;
        writer.Key(label.c_str());
        writer.Double(value);
    }

    writer.Key("100");
    writer.Double(max);

    writer.EndObject();

    Document doc;
    doc.Parse(buffer.GetString());
    return doc;
}

}
