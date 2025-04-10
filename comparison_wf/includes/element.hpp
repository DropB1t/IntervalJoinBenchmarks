/**************************************************************************************
 *  Copyright (c) 2024- Gabriele Mencagli
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

#ifndef ELEMENT_H
#define ELEMENT_H

#include<string>

struct Element
{
    std::string key;
    std::string val;
    uint64_t ts;

    // Constructor I
    Element(const std::string &_key,
            const std::string &_val,
            uint64_t _ts):
            key(_key),
            val(_val),
            ts(_ts) {}

    // Constructor II
    Element() {}
};

#endif
