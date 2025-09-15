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

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import join.sources.StreamSource;
import constants.IntervalJoinConstants;
import util.Log;
import util.MetricGroup;
import util.ThroughputCounter;
import util.Util;

public class ComparisonFlink {
    private static final Logger LOG = Log.get(ComparisonFlink.class);
    private static int probe_dataset_size = 2000000;
    private static int base_dataset_size = 1000000;
    private static int numKeys = 0;

    public static void main(String[] args) throws Exception {
        String alert = "Parameters: --parallelism <nSource1,nSource2,nInterval-Join,nSink> --keys <num_keys> -win <window in usec>\n";
        if (args.length == 1 && args[0].equals(IntervalJoinConstants.HELP)) {
            System.out.print(alert);
            System.exit(0);
        }
        // load configuration
        ParameterTool props;
        Configuration conf;
        try {
            props = ParameterTool.fromPropertiesFile(ComparisonFlink.class.getResourceAsStream(IntervalJoinConstants.DEFAULT_PROPERTIES));
            conf = props.getConfiguration();
            LOG.debug("Loaded configuration file: " + conf.toString());
        }
        catch (IOException e) {
            LOG.error("Unable to load configuration file", e);
            throw new RuntimeException("Unable to load configuration file", e);
        }
        ParameterTool argsTool = ParameterTool.fromArgs(args);
        if (!argsTool.has("parallelism") || !argsTool.has("keys") || !argsTool.has("win")) {
            LOG.error("Error in parsing the input arguments");
            LOG.error(alert);
            System.exit(1);
        }
        int samplingRate = 100000;
        numKeys = argsTool.getInt("keys", 50);
        int lower_bound = argsTool.getInt("win", 5000);
        int upper_bound = 0;
        int[] parallelism_degs = ToIntArray(argsTool.get("parallelism").split(","));
        if (parallelism_degs.length != 4) {
            LOG.error("Please provide 4 parallelism degrees");
            System.exit(1);
        }
        int source1_deg = parallelism_degs[0];
        int source2_deg = parallelism_degs[1];
        int join_deg = parallelism_degs[2];
        int sink_deg = parallelism_degs[3];
        long maxTs = Math.max(base_dataset_size, probe_dataset_size);
        ArrayList<Element> base_datasets, probe_datasets;
        RichParallelSourceFunction<Tuple3<String, String, Long>> baseSource, probeSource;
        base_datasets = generateStream(base_dataset_size, numKeys, maxTs);
        probe_datasets = generateStream(probe_dataset_size, numKeys, maxTs);
        baseSource = new StreamSource(base_datasets);
        probeSource = new StreamSource(probe_datasets);
        // Set up the streaming execution Environment
        Configuration flink_config = new Configuration();
        flink_config.set(TaskManagerOptions.TASK_HEAP_MEMORY , MemorySize.ofMebiBytes(15360));
        flink_config.set(TaskManagerOptions.NUM_TASK_SLOTS, 64);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(flink_config);
        env.getConfig().disableGenericTypes();

        DataStream<Tuple3<String, String, Long>> baseStream = env.addSource(baseSource)
                                                    .setParallelism(source1_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple3<String, String, Long>> probeStream = env.addSource(probeSource)
                                                    .setParallelism(source2_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        /* DataStream<Tuple3<String, String, Long>> joinedStream = baseStream
                                                .keyBy(new DataKeySelector())
                                                .intervalJoin(probeStream.keyBy(new DataKeySelector()))
                                                .between(Time.milliseconds(-1*lower_bound/1000), Time.milliseconds(0))
                                                .process(new IntervalJoin()).setParallelism(join_deg); */

        DataStream<Tuple3<String, String, Long>> joinedStream = baseStream.join(probeStream)
                    .where(tuple -> tuple.f0)
                    .equalTo(tuple -> tuple.f0)
                    .window(SlidingEventTimeWindows.of(Time.milliseconds(lower_bound/1000), Time.milliseconds(100)))
                    .with(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple3<String, String, Long>> (){
                        @Override
                        public Tuple3<String, String, Long> join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) {
                            Tuple3<String, String, Long> out_t = new Tuple3<String, String, Long>();
                            Long max_ts = Math.max(first.f2, second.f2);
                            out_t.f0 = first.f0;
                            out_t.f1 = first.f1 + second.f1;
                            out_t.f2 = max_ts;
                            return out_t;
                        }
                    }).setParallelism(join_deg);

        joinedStream.addSink(new ConsoleSink(samplingRate)).setParallelism(sink_deg);

        LOG.info(" Test Interval_Join (comparison with Flink) prepares to run...\n" +
        "  * num_keys: " + numKeys + "\n" +
        "  * source1: " + source1_deg + "\n" +
        "  * source2: " + source2_deg + "\n" +
        "  * join: " + join_deg + "\n" +
        "  * sink: " + sink_deg + "\n" +
        "  * \n" +
        "  * TOPOLOGY\n" +
        "  * ==============================\n" +
        "  * source1 +--+ \n" + 
        "  *            +--> join --> sink \n" + 
        "  * source2 +--+ \n" + 
        "  * ==============================\n");
        try {
            // run the topology
            //LOG.info("Executing " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " topology");
            JobExecutionResult result = env.execute();
            //LOG.info("Exiting");
            // measure throughput
            double throughput = (double) (ThroughputCounter.getValue() / result.getNetRuntime(TimeUnit.SECONDS));
            LOG.info("Measured throughput: " + throughput + " tuples/second");
            // dumpThroughput((int)throughput);
            LOG.info("Dumping metrics");
            MetricGroup.dumpAll();
        }
        catch (Exception e) {
            LOG.error(e.toString());
        }
        LOG.info("...end\n");
    }

    public static ArrayList<Element> generateStream(int count, int keyNum, long maxTs) {
        if (maxTs < count - 1) {
            maxTs = count - 1;
        }
        ArrayList<Element> stream = new ArrayList<>(count);
        long step = maxTs / count;
        if (step < 1) {
            step = 1;
        }
        int countPerKey = count / keyNum;
        if (countPerKey * keyNum != count) {
            System.err.println("Element count is not divided by keyNum!");
            System.exit(1);
        }
        for (int i = 0; i < countPerKey; i++) {
            for (int k = 0; k < keyNum; k++) {
                int idx = i * keyNum + k;
                long ts = (idx) * step;
                stream.add(new Element("key_" + k, Long.toString(ts), ts));
            }
        }
        return stream;
    }

    private static void dumpThroughput(int throughput) throws JsonProcessingException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode throughputNode = objectMapper.convertValue(throughput, JsonNode.class);
        Util.appendJson(throughputNode, "throughput.json");
    }

    private static int[] ToIntArray(String[] stringArray) {
        return Stream.of(stringArray).mapToInt(Integer::parseInt).toArray();
    }

    private static class DataKeySelector implements KeySelector<Tuple3<String, String, Long>, String> {
        @Override
        public String getKey(Tuple3<String, String, Long> value) {
            return value.f0;
        }
    }

    private static class IngestionTimeWatermarkStrategy implements WatermarkStrategy<Tuple3<String, String, Long>> {
        private IngestionTimeWatermarkStrategy() {}

        public static IngestionTimeWatermarkStrategy create() {
            return new IngestionTimeWatermarkStrategy();
        }

        @Override
        public WatermarkGenerator<Tuple3<String, String, Long>> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<Tuple3<String, String, Long>> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> timestamp;
        }
    }
}
