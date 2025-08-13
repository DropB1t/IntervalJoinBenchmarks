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

#include "../includes/util/sampler.h"
#include <windflow.hpp>
#include <iostream>

namespace util {

Sampler::Sampler(long samples_per_second)
    : samples_per_second_(samples_per_second)
    , epoch_(wf::current_time_nsecs())
    , counter_(0)
    , total_(0)
{}

void Sampler::add(double value, unsigned long timestamp)
{
    ++total_;

    // add samples according to the sample rate
    auto seconds = (timestamp - epoch_) / 1e09;
    if (samples_per_second_ == 0 || counter_ <= samples_per_second_ * seconds) {
        samples_.push_back(value);
        ++counter_;
    }
}

const std::vector<double> &Sampler::values() const
{
    return samples_;
}

long Sampler::total() const
{
    return total_;
}

}
