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
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import join.SourceEvent;
import util.ThroughputCounter;

import java.util.ArrayList;

public class FileSource extends RichParallelSourceFunction<SourceEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);

    private long t_start;
    private long t_end;

    private ArrayList<SourceEvent> dataset;
    private int data_size;
    
    private final long runtime;
    private boolean running = true;
    
    private final int gen_rate;
    private long nt_execution;
    private long generated;
    private int index;
    
    private long ts = 1704106800000L; // January 1, 2024 12:00:00 AM in ms

    public FileSource(long _runtime, int _gen_rate, ArrayList<SourceEvent> _dataset) {
        this.runtime = (long) (_runtime * 1e9); // ns
        this.gen_rate = _gen_rate;

        this.dataset = _dataset;
        
        index = 0;
        generated = 0;
        data_size = dataset.size();
        nt_execution = 0;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(final SourceContext<SourceEvent> ctx) throws Exception {
        this.t_start = System.nanoTime();

        // generation loop
        while ((System.nanoTime() - this.t_start < runtime) && running) {
            SourceEvent tuple = dataset.get(index);
            ts += tuple.f2 != 0L ? tuple.f2 : 500L;
            
            SourceEvent input = new SourceEvent();
            input.f0 = tuple.f0;
            input.f1 = tuple.f1;
            input.f2 = System.nanoTime();
            ctx.collectWithTimestamp(input, ts);
            /* 
            if (generated < 15)
                LOG.info("  * key-> " + tuple.f0 + ", ts-> " + ts);
             */
            generated++;
            index++;
            
            if (gen_rate != 0) { // limit generation rate with active delay
                long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
                active_delay(delay_nsec);
            }
            
            if (index >= data_size) { // check the dataset boundaries
                index = 0;
                nt_execution++;
            }
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
                    ", generations: " + nt_execution +
                    ", bandwidth: " + rate +  // tuples per second
                    " tuples/s");
        }
    }
}
