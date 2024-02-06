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

#ifndef IJ_CLI_UTIL_HPP
#define IJ_CLI_UTIL_HPP

#include<iomanip>
#include<iostream>
#include<string>
#include<vector>
#include<getopt.h>
#include "constants.hpp"

using namespace std;

typedef enum { NONE, REQUIRED } opt_arg;    // an option can require one argument or none

const struct option long_opts[] = {
        {"help", NONE, 0, 'h'},
        {"rate", REQUIRED, 0, 'r'},      // pipe start (source) parallelism degree
        {"key", REQUIRED, 0, 'k'},
        {"sampling", REQUIRED, 0, 's'},   // predictor parallelism degree
        {"batch", REQUIRED, 0, 'b'},
        {"parallelism", REQUIRED, 0, 'p'},        // pipe end (sink) parallelism degree
        {"type", REQUIRED, 0, 't'},        // type of test to run
        {"lower", REQUIRED, 0, 'l'},        // lower bound of interval
        {"upper", REQUIRED, 0, 'u'},        // upper bound of interval
        {"chaining", NONE, 0, 'c'},
        {0, 0, 0, 0}
};

const string command_help = "Parameters: --rate <value> --key <value> --sampling <value> --batch <size> --parallelism <nRSource,nLSource,nJoin,nSink> --type < su | sz | rd | sd > -l [lower bound in usec] -u [upper bound in usec] [--chaining]";

// information about application
const string rsource_str = "  * rsource parallelism degree: ";
const string lsource_str = "  * lsource parallelism degree: ";
const string join_str = "  * join parallelism degree: ";
const string sink_str = "  * sink parallelism degree: ";

const string app_descr = "Submiting IntervalJoinBenchmark with parameters:";
const string app_error = "Error executing IntervalJoinBenchmark topology";
const string app_termination = "Terminated execution of IntervalJoinBenchmark topology with cardinality ";

#endif //IJ_CLI_UTIL_HPP
