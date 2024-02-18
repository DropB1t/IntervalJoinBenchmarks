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

#ifndef GEN_UTIL_HPP
#define GEN_UTIL_HPP

#include <iomanip>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <getopt.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "constants.hpp"

using namespace std;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"num_key", REQUIRED, 0, 'k'},
        {"size", REQUIRED, 0, 's'},
        {"zipf", REQUIRED, 0, 'z'},
        {"type", REQUIRED, 0, 't'},         // type of test to run
        {0, 0, 0, 0}
};

const string command_help = "Parameters: --num_key <value> [--size <dataset_size>] --type < su | sz > [--zipf <zipf_exponent>]"
                            "\n\nOptions:"
                            "\n\t--num_key <value> : number of keys"
                            "\n\t--size <dataset_size> : size of the dataset (default: 200000)"
                            "\n\t--type < su | sz > : type of test to run"
                            "\n\t--zipf <zipf_exponent> : zipf exponent (default: 0.8)"
                            "\n\t--help : print this help message";

const string dataset_types = "Types:"
                             "\n\tsu = synthetic dataset with uniform distribution"
                             "\n\tsz = synthetic dataset with zipf distribution";

const string parse_error = "Error in parsing the input arguments";

const string gen_descr = "Generating dataset with submitted parameters:";

std::string base_name(std::string const & path)
{
  return path.substr(path.find_last_of("\\/") + 1);
}

std::string dir_path(std::string const & path)
{
  return path.substr(0,path.find_last_of("\\/"));
}

#endif //GEN_UTIL_HPP
