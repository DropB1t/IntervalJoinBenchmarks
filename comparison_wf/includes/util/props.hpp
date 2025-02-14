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

#ifndef IJ_CONSTANTS_HPP
#define IJ_CONSTANTS_HPP

#include<string>

using namespace std;

/// application run time (source generates the stream for app_run_time seconds, then sends out EOS)
const unsigned long app_run_time = 60 * 1000000000L; // 60 seconds

/// components and topology name
const string topology_name = "IntervalJoinBenchmark";
const string r_source_name = "rsource";
const string l_source_name = "lsource";
const string join_name = "join";
const string sink_name = "sink";

static const char *types_str[] = {"Synthetic Test (Uniform Distribution)", "Synthetic Test (ZipF Distribution)", "Synthetic Test (SelfSimilar Distribution)", "Rovio Dataset", "Stock Dataset"};
static const char *modes_str[] = {"Key based Parallelism", "Data based Parallelism", "Hybrid based Parallelism"};

// Datasets path to be used

// Rovio
const string rovio_path = "../datasets/rovio/1000ms_1t.txt";

// Stock
const string r_stock_path ="../datasets/stock/cj_1000ms_1t.txt";
const string l_stock_path ="../datasets/stock/sb_1000ms_1t.txt";

// Synthetic Uniform
const string r_synthetic_uniform_path ="../datasets/synt_u/r_synthetic_uniform_timestamped.txt";
const string l_synthetic_uniform_path ="../datasets/synt_u/l_synthetic_uniform_timestamped.txt";

// Synthetic ZipF
const string r_synthetic_zipf_path ="../datasets/synt_z/r_synthetic_zipf_timestamped.txt";
const string l_synthetic_zipf_path ="../datasets/synt_z/l_synthetic_zipf_timestamped.txt";

// Synthetic SelfSimilar
const string r_synthetic_ss_path ="../datasets/synt_ss/r_synthetic_ss_timestamped.txt";
const string l_synthetic_ss_path ="../datasets/synt_ss/l_synthetic_ss_timestamped.txt";

const char split_char = '|';

#endif //IJ_CONSTANTS_HPP
