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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import util.Log;

public class IntervalJoin extends ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple3<String, String, Long>> {
    private static final Logger LOG = Log.get(IntervalJoin.class);
    private long processed;

    @Override
    public void open(Configuration parameters) throws Exception {
        processed = 0;
    }

    @Override
    public void processElement(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second, Context ctx, Collector<Tuple3<String, String, Long>> out) throws Exception {
        Tuple3<String, String, Long> out_t = new Tuple3<String, String, Long>();
        Long max_ts = Math.max(first.f2, second.f2);
        out_t.f0 = first.f0;
        out_t.f1 = first.f1 + second.f1;
        out_t.f2 = max_ts;
        out.collect(out_t);
        processed++;
    }

    @Override
    public void close() {}
}
