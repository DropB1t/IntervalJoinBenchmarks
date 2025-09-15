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

package join;

import util.Log;
import util.Sampler;
import util.MetricGroup;
import org.slf4j.Logger;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/** 
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  @version August 2024
 *  
 *  Sink node that receives and prints the results.
 */ 
public class ConsoleSink extends RichSinkFunction<Tuple3<String, String, Long>> {
    private static final Logger LOG = Log.get(ConsoleSink.class);
    private long processed;
    private Sampler latency;
    private final long samplingRate;

    // Constructor
    public ConsoleSink(long _samplingRate) {
        samplingRate = _samplingRate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        processed = 0;               // total number of processed tuples
        latency = new Sampler(samplingRate);
    }

    @Override
    public void invoke(Tuple3<String, String, Long> input, Context context) throws Exception {
        String key = input.f0;
        String val = input.f1;
        long timestamp = input.f2;
        System.out.println("" + key + "\t" + val + "\t" + (context.timestamp()));
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)((now - timestamp)/ 1e3), System.nanoTime()); // us precision
        processed++;
    }

    @Override
    public void close() {
        MetricGroup.add("latency", latency);
    }
}
