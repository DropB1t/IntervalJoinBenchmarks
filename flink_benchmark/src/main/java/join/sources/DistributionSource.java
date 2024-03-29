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

package join.sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import util.ThroughputCounter;

public class DistributionSource extends RichParallelSourceFunction<Tuple3<Integer, Integer, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSource.class);
    
    static final int VALUE = 5;

    private long t_start;
    private long t_end;

    private int num_keys;
    
    private final long runtime;
    private boolean running = true;
    
    private final int gen_rate;
    private long generated;
    
    private long ts = 1704106800000L; // January 1, 2024 12:00:00 AM in ms
    private final int offset_seed;
    private MersenneTwister offset;
    private UniformIntegerDistribution keyDistribution;

    public DistributionSource(long _runtime, int _gen_rate, int _num_keys, int _offset_seed) {
        this.runtime = (long) (_runtime * 1e9); // ns
        this.offset_seed = _offset_seed;
        this.gen_rate = _gen_rate;
        this.num_keys = _num_keys;
        generated = 0;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.offset = new MersenneTwister(offset_seed);
        this.keyDistribution = new UniformIntegerDistribution(offset, 1, num_keys);
    }

    @Override
    public void run(final SourceContext<Tuple3<Integer, Integer, Long>> ctx) throws Exception {
        this.t_start = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - this.t_start < runtime) && running) {
            Tuple3<Integer, Integer, Long> tuple = new Tuple3<Integer, Integer, Long>();
            tuple.f0 = keyDistribution.sample();
            tuple.f1 = VALUE;
            tuple.f2 = System.nanoTime();
            ctx.collectWithTimestamp(tuple, ts);
            generated++;
            
            if (gen_rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
                active_delay(delay_nsec);
            }

            ts += offset.nextInt(500);
        }

        // terminate the generation
        running = false;
        this.t_end = System.nanoTime();
        ThroughputCounter.add(generated);
    }

    /**
     * Add some active delay (busy-waiting function).
     * @param nsecs wait time in nanoseconds
     */
    private void active_delay(double nsecs) {
        long t_start = System.nanoTime();
        long t_now;
        boolean end = false;
        while (!end) {
            t_now = System.nanoTime();
            end = (t_now - t_start) >= nsecs;
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    // close method
    @Override
    public void close() {
        if (generated == 0) {
            LOG.info("[Source] processed tuples: " + generated);
        } else {
            long t_elapsed = (this.t_end - this.t_start) / 1000000; // elapsed time in milliseconds
            double rate = Math.floor( generated / ((double)(this.t_end - this.t_start) / 1e9) ); // per second
            LOG.info("[Source] execution time: " + t_elapsed +
                    " ms, generated: " + generated +
                    ", bandwidth: " + rate +  // tuples per second
                    " tuples/s");
        }
    }
}
