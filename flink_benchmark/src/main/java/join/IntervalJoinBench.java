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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;

import join.sources.DistributionSource;
import join.sources.FileSource;
import constants.IntervalJoinConstants;
import constants.IntervalJoinConstants.Conf;
import util.Log;
import util.MetricGroup;
import util.ThroughputCounter;

public class IntervalJoinBench {
    private static final Logger LOG = Log.get(IntervalJoinBench.class);

    public static void main(String[] args) throws Exception {

        String alert = "Parameters: --rate <value> --sampling <value> --parallelism <nSource1,nSource2,nInterval-Join,nSink> --type < su | sz | rd | sd > [--chaining]\n" + 
                       "Types:\n\tsu = synthetic dataset with uniform distribution" + 
                       "\n\tsz = synthetic dataset with zipf distribution" + 
                       "\n\trd = rovio dataset" + 
                       "\n\tsd = stock dataset\n\n";

        if (args.length == 1 && args[0].equals(IntervalJoinConstants.HELP)) {
            System.out.print(alert);
            System.exit(0);
        }

        // load configuration
        ParameterTool props;
        Configuration conf;
        try {
            props = ParameterTool.fromPropertiesFile(IntervalJoinBench.class.getResourceAsStream(IntervalJoinConstants.DEFAULT_PROPERTIES));
            conf = props.getConfiguration();
            LOG.debug("Loaded configuration file: " + conf.toString());
        }
        catch (IOException e) {
            LOG.error("Unable to load configuration file", e);
            throw new RuntimeException("Unable to load configuration file", e);
        }
        
        int runtime = conf.getInteger(ConfigOptions.key(Conf.RUNTIME).intType().defaultValue(60)); // runtime in seconds - default 1 min
        int dataSize = conf.getInteger(ConfigOptions.key(Conf.DATA_SIZE).intType().defaultValue(2000000));
        
        int lower_bound = conf.getInteger(ConfigOptions.key(Conf.LOWER_BOUND).intType().defaultValue(-500));
        int upper_bound = conf.getInteger(ConfigOptions.key(Conf.UPPER_BOUND).intType().defaultValue(500));
        int num_keys = conf.getInteger(ConfigOptions.key(Conf.NUM_KEYS).intType().defaultValue(10));

        int rseed = conf.getInteger(ConfigOptions.key(Conf.RSEED).intType().defaultValue(12345));
        int lseed = conf.getInteger(ConfigOptions.key(Conf.LSEED).intType().defaultValue(54321));
        
        ParameterTool argsTool = ParameterTool.fromArgs(args);

        if (!argsTool.has("rate") || !argsTool.has("sampling") || !argsTool.has("type") || !argsTool.has("parallelism")) {
            LOG.error("Error in parsing the input arguments");
            LOG.error(alert);
            System.exit(1);
        }

        int rate = argsTool.getInt("rate", 0);
        int samplingRate = argsTool.getInt("sampling", 100);
        String type = argsTool.get("type", "su");
        boolean chaining = argsTool.has("chaining");

        int[] parallelism_degs = ToIntArray(argsTool.get("parallelism").split(","));
        if (parallelism_degs.length != 4) {
            LOG.error("Please provide 4 parallelism degrees");
            System.exit(1);
        }

        int source1_deg = parallelism_degs[0];
        int source2_deg = parallelism_degs[1];
        int join_deg = parallelism_degs[2];
        int sink_deg = parallelism_degs[3];

        int distribution_seed = conf.getInteger(ConfigOptions.key(Conf.SEED).intType().defaultValue(441287210));
        double exponent = conf.getDouble(ConfigOptions.key(Conf.ZIPF_EXPONENT).doubleType().defaultValue(1.1));
        Well19937c rnd = new Well19937c(distribution_seed);

        RichParallelSourceFunction<Event> orangeSource, greenSource;
        switch (type) {
            case "sz":
                ZipfDistribution zDistribution = new ZipfDistribution(rnd, num_keys, exponent);
                orangeSource = new DistributionSource(runtime, rate, rseed, zDistribution.sample(dataSize), dataSize);
                greenSource = new DistributionSource(runtime, rate, lseed, zDistribution.sample(dataSize), dataSize);
                break;
            case "rd":
                String file_path = conf.get(ConfigOptions.key(Conf.ROVIO_PATH).stringType().noDefaultValue());
                orangeSource = new FileSource(file_path, runtime, rate, rseed);
                greenSource = new FileSource(file_path, runtime, rate, lseed);
                break;
            case "sd":
                String rpath = conf.get(ConfigOptions.key(Conf.RSTOCK_PATH).stringType().noDefaultValue());
                String lpath = conf.get(ConfigOptions.key(Conf.LSTOCK_PATH).stringType().noDefaultValue());
                orangeSource = new FileSource(rpath, runtime, rate, rseed);
                greenSource = new FileSource(lpath, runtime, rate, lseed);
                break;
            case "su":
            default:
                UniformIntegerDistribution uDistribution = new UniformIntegerDistribution(rnd, 1, num_keys);
                orangeSource = new DistributionSource(runtime, rate, rseed, uDistribution.sample(dataSize), dataSize);
                greenSource = new DistributionSource(runtime, rate, lseed, uDistribution.sample(dataSize), dataSize);
                break;
        }

        // Set up the streaming execution Environment
        Configuration conf_flink = new Configuration();
        conf_flink.set(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf_flink);
        env.getConfig().disableGenericTypes();
       
        DataStream<Event> orangeStream = env.addSource(orangeSource)
                                                    .setParallelism(source1_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Event> greenStream = env.addSource(greenSource)
                                                    .setParallelism(source2_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Event> joinedStream = orangeStream
                                                .keyBy(new DataKeySelector())
                                                .intervalJoin(greenStream.keyBy(new DataKeySelector()))
                                                .between(Time.milliseconds(lower_bound), Time.milliseconds(upper_bound))
                                                .process( new IntervalJoin() ).setParallelism(join_deg);

        joinedStream.addSink(new ConsoleSink(samplingRate)).setParallelism(sink_deg);

        /* Filter Test
        DataStream<Source_Event> filtered = orangeStream.filter(new FilterTest()).setParallelism(1);
        filtered.addSink(new ConsoleSink(100)).setParallelism(1);
        */

        String print_type = "";
        switch (type) {
            case "sz":
                print_type = "Synthetic Test (ZipF Distribution)";
                break;
            case "rd":
                print_type = "Rovio Dataset";
                break;
            case "sd":
                print_type = "Stock Dataset";
                break;
            case "su":
            default:
                print_type = "Synthetic Test (Uniform Distribution)";
                break;
        }

        String synthetic_stats =
        "  * data_size: " + dataSize + "\n" +
        "  * num_keys: " + num_keys + "\n";

        // print app info
        LOG.info("Submiting " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " with parameters:\n\n" +
        "  * rate: " + ((rate == 0) ? "full_speed" : rate) + " tuples/second\n" +
        "  * sampling: " + samplingRate + "\n" +
        ((type.equals("su") || type.equals("sz")) ? synthetic_stats : "") +
        "  * \n" +
        "  * source1: " + source1_deg + "\n" +
        "  * source2: " + source2_deg + "\n" +
        "  * join: " + join_deg + "\n" +
        "  * sink: " + sink_deg + "\n" +
        "  * \n" +
        "  * lower_bound: " + lower_bound + "\n" +
        "  * upper_bound: " + upper_bound + "\n" +
        "  * type: " + print_type + "\n" +
        "  * \n" +
        "  * TOPOLOGY\n" +
        "  * ==============================\n" +
        "  * source1 +--+ \n" + 
        "  *            +--> join --> sink \n" + 
        "  * source2 +--+ \n" + 
        "  * ==============================\n" );

        try {
            if (!chaining) {
                env.disableOperatorChaining();
                LOG.info("Chaining is disabled");
            }
            else {
                LOG.info("Chaining is enabled");
            }

            // run the topology
            LOG.info("Executing " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " topology");
            JobExecutionResult result = env.execute();
            LOG.info("Exiting");

            // measure throughput
            double throughput = (double) (ThroughputCounter.getValue() / result.getNetRuntime(TimeUnit.SECONDS));
            LOG.info("Measured throughput: " + throughput + " tuples/second");

            LOG.info("Dumping metrics");
            MetricGroup.dumpAll();
        }
        catch (Exception e) {
            LOG.error(e.toString());
        }

    }

    private static int[] ToIntArray(String[] stringArray) {
        return Stream.of(stringArray).mapToInt(Integer::parseInt).toArray();
    }

    private static class DataKeySelector implements KeySelector<Event, Integer> {
        @Override
        public Integer getKey(Event value) {
            return value.f0;
        }
    }

    private static class IngestionTimeWatermarkStrategy implements WatermarkStrategy<Event> {

        private IngestionTimeWatermarkStrategy() {}

        public static IngestionTimeWatermarkStrategy create() {
            return new IngestionTimeWatermarkStrategy();
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> timestamp;
        }
    }

}
