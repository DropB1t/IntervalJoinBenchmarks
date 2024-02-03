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

#ifndef WORDCOUNT_RESULT_HPP
#define WORDCOUNT_RESULT_HPP

#include<windflow.hpp>

using namespace std;

struct tuple_t
{
    size_t key;
    int64_t value;
    uint64_t ts;

    // Default Constructor
    tuple_t():
        key(0),
        value(0),
        ts(0) {}

    // Constructor I
    tuple_t(size_t _key):
            key(_key),
            value(5),
            ts(0) {}

    // Constructor II
    tuple_t(size_t _key, int64_t _value):
            key(_key),
            value(_value),
            ts(0) {}
};

#if 1
template<>
struct std::hash<tuple_t>
{
    size_t operator()(const tuple_t &t) const
    {
        return std::hash<int>()(t.value) + std::hash<int>()(t.key);
    }
};
#endif

#endif //WORDCOUNT_RESULT_HPP
