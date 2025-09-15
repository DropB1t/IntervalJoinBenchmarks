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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import join.Element;
import util.ThroughputCounter;
import java.util.ArrayList;

public class StreamSource extends RichParallelSourceFunction<Tuple3<String, String, Long>> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamSource.class);
    private ArrayList<Element> dataset;
    private int data_size;
    private boolean running = true;
    private long generated;
    private int index;

    public StreamSource(ArrayList<Element> _dataset) {
        this.dataset = _dataset;
        index = 0;
        generated = 0;
        data_size = dataset.size();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(final SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        // generation loop
        for (index = 0; index < data_size; index++) {
            Element elem = dataset.get(index);       
            ctx.collectWithTimestamp(new Tuple3<>(elem.key, elem.value, System.nanoTime()), elem.timestamp + 125);
            generated++;
        }
        // terminate the generation
        running = false;
        ThroughputCounter.add(generated);
    }

    @Override
    public void cancel() {
        running = false;
    }

    // close method
    @Override
    public void close() {}
}
