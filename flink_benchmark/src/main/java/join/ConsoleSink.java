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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/** 
 *  @author  Gabriele Mencagli and Yuriy Rymarchuk
 *  @version August 2024
 *  
 *  Sink node that receives and prints the results.
 */ 
public class ConsoleSink extends RichSinkFunction<SourceEvent> {

    private static final Logger LOG = Log.get(ConsoleSink.class);
    private long processed;
    private long t_start;
    private long t_end;
    private Sampler latency;
    private final long samplingRate;

    // Constructor
    public ConsoleSink(long _samplingRate) {
        samplingRate = _samplingRate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
        latency = new Sampler(samplingRate);
    }

    @Override
    public void invoke(SourceEvent input, Context context) throws Exception {
        //int key = input.f0;
        //int value = input.f1;
        long timestamp = input.f2;
        /*         
        if (processed < 15)
            LOG.info("  * key-> " + key + ", ts-> " + context.timestamp());
        */
        // evaluate latency
        long now = System.nanoTime();
        latency.add((double)((now - timestamp)/ 1e6), System.nanoTime()); // ms precision
        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void close() {
        if (processed == 0) {
            LOG.info("[Sink] processed tuples: " + processed);
        }
        else {
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            //LOG.info("exec time " + t_elapsed + " || in seconds " + ((double)t_elapsed / 1000) );
            LOG.info("[Sink] execution time: " + t_elapsed +
                    " ms, processed: " + processed +
                    ", bandwidth: " + Math.floor(processed / ((double)t_elapsed / 1000)) +  // tuples per second
                    " tuples/s");
            MetricGroup.add("latency", latency);
        }
    }
}
