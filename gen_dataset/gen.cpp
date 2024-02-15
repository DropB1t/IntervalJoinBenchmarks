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

using namespace std;

#include "includes/zipfian_int_distribution.h"
#include "includes/util.hpp"

gen_types type;

void generate_dataset(const string &path, int num_keys, int size, uint seed)
{
    mt19937 engine(seed);
    auto uniform_key_gen = uniform_int_distribution<int>(1, num_keys);
    auto zipf_key_gen = zipfian_int_distribution<int>(1, num_keys, zipf_exponent);

    auto ts_offset = uniform_int_distribution<int>(0, 500);
    
    string dir = dir_path(path);
    if (mkdir(dir.c_str(), 0777) != 0) { // create the dataset directory
        struct stat st;
        if((stat(dir.c_str(), &st) != 0) || !S_ISDIR(st.st_mode)) {
            std::cerr <<  "Error: directory for dataset files cannot be created" << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    ofstream ds(path);

    if (ds.is_open()) {
        ds << num_keys << separator << size << endl;
        for (int i = 0; i < size; i++)
        {
            int key = ( type == gen_types::UNIFORM_SYNTHETIC ? uniform_key_gen(engine) : zipf_key_gen(engine) );
            ds << key << separator << ts_offset(engine) << endl;
        }
    } else {
        cout << "Unable to open " << path << " file";
    }

    ds.close();
}

int main(int argc, char *argv[])
{
    /// parse arguments from command line
    int option = 0;
    int index = 0;

    string rpath;
    string lpath;
    
    size_t data_size = 0;
    size_t num_keys = 0;

    if (argc == 5 || argc == 7) {
        while ((option = getopt_long(argc, argv, "k:s:t:", long_opts, &index)) != -1) {
            switch (option) {
                case 'k': {
                    num_keys = atoi(optarg);
                    break;
                }
                case 's': {
                    data_size = atoi(optarg);
                    break;
                }
                case 't': {
                    string str_type(optarg);
                    if (str_type == "su") {
                        type = UNIFORM_SYNTHETIC;
                        rpath = r_synthetic_uniform_path;
                        lpath = l_synthetic_uniform_path;
                    } else if (str_type == "sz") {
                        type = ZIPF_SYNTHETIC;
                        rpath = r_synthetic_zipf_path;
                        lpath = l_synthetic_zipf_path;
                    } else {
                        type = UNIFORM_SYNTHETIC;
                        rpath = r_synthetic_uniform_path;
                        lpath = l_synthetic_uniform_path;
                    }
                    break;
                }
                default: {
                    cout << parse_error << endl;
                    exit(EXIT_FAILURE);
                }
            }
        }
    } else if (argc == 2 && ((option = getopt_long(argc, argv, "h", long_opts, &index)) != -1) && option == 'h') {
        cout << command_help << endl;
        cout << dataset_types << endl;
        exit(EXIT_SUCCESS);
    } else {
        cout << parse_error << endl;
        cout << command_help << endl;
        exit(EXIT_FAILURE);
    }

    if (data_size == 0) {
        data_size = default_data_size;
    }
    
    // display test configuration
    cout << gen_descr << endl;
    cout << "  * data_size: " << data_size << endl;
    cout << "  * number of keys: " << num_keys << endl;
    cout << "  * type: " << gentype_str[type] << endl;
    cout << "  * generated files: " << base_name(rpath) << ", " << base_name(lpath) << endl;

    generate_dataset(rpath, num_keys, data_size, rseed);
    generate_dataset(lpath, num_keys, data_size, lseed);

    cout << "Datasets are generated ✓" << endl;

    return 0;
}
