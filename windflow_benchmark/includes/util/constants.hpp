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

#ifndef WORDCOUNT_CONSTANTS_HPP
#define WORDCOUNT_CONSTANTS_HPP

#include<string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
unsigned long app_run_time = 60 * 1000000000L; // 60 seconds
const size_t data_size = 2000000;

const uint rseed = 12345;
const uint lseed = 54321;

const double zipf_exponent = 0.8;

/// components and topology name
const string topology_name = "IntervalJoinBenchmark";
const string r_source_name = "rsource";
const string l_source_name = "lsource";
const string join_name = "join";
const string sink_name = "sink";

static const char *types_str[] = {"Synthetic Test (Uniform Distribution)", "Synthetic Test (ZipF Distribution)", "Rovio Dataset", "Stock Dataset"};

// path of the dataset to be used
const string rovio_path = "../datasets/rovio/1000ms_1t.txt";

const string r_stock_path ="../datasets/stock/cj_1000ms_1t.txt";
const string l_stock_path ="../datasets/stock/sb_1000ms_1t.txt";

const char default_split_char = '|';

#endif //WORDCOUNT_CONSTANTS_HPP
