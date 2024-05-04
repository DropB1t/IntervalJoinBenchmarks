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

package constants;

/** 
 *  @author  Yuriy Rymarchuk
 *  @version January 2024
 *  
 *  Constants peculiar of the IntervalJoinBenchmarks application.
 */ 
public interface IntervalJoinConstants {
    String HELP = "--help";
    String DEFAULT_PROPERTIES = "/ij.properties";
    String DEFAULT_TOPO_NAME = "IntervalJoinBenchmark";
    String DEFAULT_SEPARATOR = "\\|";

    interface Conf {
        String RSYNT_UNIFORM_PATH = "ij.source.r_synthetic_uniform_path";
        String LSYNT_UNIFORM_PATH = "ij.source.l_synthetic_uniform_path";
        String RSYNT_ZIPF_PATH = "ij.source.r_synthetic_zipf_path";
        String LSYNT_ZIPF_PATH = "ij.source.l_synthetic_zipf_path";
        String RSTOCK_PATH = "ij.source.r_stock_path";
        String LSTOCK_PATH = "ij.source.l_stock_path";
        String ROVIO_PATH = "ij.source.rovio_path";
        String RUNTIME = "ij.runtime_sec";
    }
}
