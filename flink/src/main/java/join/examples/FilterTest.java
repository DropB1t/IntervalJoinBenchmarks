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

package join.examples;

import util.Log;
import org.slf4j.Logger;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;

// class Filter
public class FilterTest extends RichFilterFunction<Tuple3<Integer, Integer, Long>> {
    private static final Logger LOG = Log.get(FilterTest.class);
    private long t_start;
    private long t_end;
    private long processed;

    // constructor
    public FilterTest() {}

    // open method
    @Override
    public void open(Configuration cfg) {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;
    }

    // flatmap method
    @Override
    public boolean filter(Tuple3<Integer, Integer, Long> input) {
        int value = input.f1;
        t_end = System.nanoTime();
        processed++;
        if (value >= 5) {
            return true;
        }
        else {
            return false;        
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
        LOG.info("[Filter] execution time: " + t_elapsed + " ms, " +
                            "processed: " + processed + ", " +
                            "bandwidth: " + processed / (t_elapsed / 1000) + " (tuples/s)");
    }
}
