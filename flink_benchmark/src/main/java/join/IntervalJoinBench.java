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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;

import join.sources.FileSource;
import constants.IntervalJoinConstants;
import constants.IntervalJoinConstants.Conf;
import util.Log;
import util.MetricGroup;
import util.ThroughputCounter;
import util.Util;

public class IntervalJoinBench {
    private static final Logger LOG = Log.get(IntervalJoinBench.class);
    
    public static enum testType {
        UNIFORM_SYNTHETIC,
        ZIPF_SYNTHETIC,
        ROVIO_TEST,
        STOCK_TEST;
    }

    private static testType type;
    private static int dataSize = 0;
    private static int numKeys = 0;

    public static void main(String[] args) throws Exception {
        String alert = "Parameters: --rate <value> --sampling <value> --parallelism <nSource1,nSource2,nInterval-Join,nSink> --type < su | sz | rd | sd > -l <lower bound in ms> -u <upper bound in ms> [--chaining]\n" + 
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
                
        ParameterTool argsTool = ParameterTool.fromArgs(args);

        if (!argsTool.has("rate") || !argsTool.has("sampling") || !argsTool.has("type") || !argsTool.has("parallelism")) {
            LOG.error("Error in parsing the input arguments");
            LOG.error(alert);
            System.exit(1);
        }

        int rate = argsTool.getInt("rate", 0);
        int samplingRate = argsTool.getInt("sampling", 100);
        boolean chaining = argsTool.has("chaining");

        int lower_bound = argsTool.getInt("l", -500);
        int upper_bound = argsTool.getInt("u", 500);

        String typeStr = argsTool.get("type", "su");
        switch (typeStr) {
            case "rd":
                type = testType.ROVIO_TEST;
                break;
            case "sd":
                type = testType.STOCK_TEST;
                break;
            case "sz":
                type = testType.ZIPF_SYNTHETIC;
                break;
            case "su":
                type = testType.UNIFORM_SYNTHETIC;
            default:
                break;
        }

        int[] parallelism_degs = ToIntArray(argsTool.get("parallelism").split(","));
        if (parallelism_degs.length != 4) {
            LOG.error("Please provide 4 parallelism degrees");
            System.exit(1);
        }

        int source1_deg = parallelism_degs[0];
        int source2_deg = parallelism_degs[1];
        int join_deg = parallelism_degs[2];
        int sink_deg = parallelism_degs[3];

        String rpath, lpath;
        ArrayList<SourceEvent> rdataset, ldataset;
        RichParallelSourceFunction<SourceEvent> orangeSource, greenSource;

        switch (type) {
            case ZIPF_SYNTHETIC:
                rpath = conf.get(ConfigOptions.key(Conf.RSYNT_ZIPF_PATH).stringType().noDefaultValue());
                lpath = conf.get(ConfigOptions.key(Conf.LSYNT_ZIPF_PATH).stringType().noDefaultValue());
                break;
            case ROVIO_TEST:
                rpath = conf.get(ConfigOptions.key(Conf.ROVIO_PATH).stringType().noDefaultValue());
                lpath = conf.get(ConfigOptions.key(Conf.ROVIO_PATH).stringType().noDefaultValue());
                break;
            case STOCK_TEST:
                rpath = conf.get(ConfigOptions.key(Conf.RSTOCK_PATH).stringType().noDefaultValue());
                lpath = conf.get(ConfigOptions.key(Conf.LSTOCK_PATH).stringType().noDefaultValue());
                break;
            case UNIFORM_SYNTHETIC:
            default:
                rpath = conf.get(ConfigOptions.key(Conf.RSYNT_UNIFORM_PATH).stringType().noDefaultValue());
                lpath = conf.get(ConfigOptions.key(Conf.LSYNT_UNIFORM_PATH).stringType().noDefaultValue());
                break;
        }

        rdataset = parseDataset(rpath, IntervalJoinConstants.DEFAULT_SEPARATOR);
        ldataset = parseDataset(lpath, IntervalJoinConstants.DEFAULT_SEPARATOR);

        orangeSource = new FileSource(runtime, rate, rdataset);
        greenSource = new FileSource(runtime, rate, ldataset);

        // Set up the streaming execution Environment
        Configuration conf_flink = new Configuration();
        conf_flink.set(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 0.5f);
        conf_flink.set(TaskManagerOptions.NUM_TASK_SLOTS, 64);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(conf_flink);
        env.getConfig().disableGenericTypes();
       
        DataStream<SourceEvent> orangeStream = env.addSource(orangeSource)
                                                    .setParallelism(source1_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<SourceEvent> greenStream = env.addSource(greenSource)
                                                    .setParallelism(source2_deg)
                                                    .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<SourceEvent> joinedStream = orangeStream
                                                .keyBy(new DataKeySelector())
                                                .intervalJoin(greenStream.keyBy(new DataKeySelector()))
                                                .between(Time.milliseconds(lower_bound), Time.milliseconds(upper_bound))
                                                .process( new IntervalJoin() ).setParallelism(join_deg);

        joinedStream.addSink(new ConsoleSink(samplingRate)).setParallelism(sink_deg);

        String print_type = "";
        switch (type) {
            case ZIPF_SYNTHETIC:
                print_type = "Synthetic Test (ZipF Distribution)";
                break;
            case ROVIO_TEST:
                print_type = "Rovio Dataset";
                break;
            case STOCK_TEST:
                print_type = "Stock Dataset";
                break;
            case UNIFORM_SYNTHETIC:
            default:
                print_type = "Synthetic Test (Uniform Distribution)";
                break;
        }

        String synthetic_stats =
        "  * data_size: " + dataSize + "\n" +
        "  * num_keys: " + numKeys + "\n";

        // print app info
        LOG.info("Submiting " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " with parameters:\n" +
        "  * rate: " + ((rate == 0) ? "full_speed" : rate) + " tuples/second\n" +
        "  * sampling: " + samplingRate + "\n" +
        (( type == testType.UNIFORM_SYNTHETIC || type == testType.ZIPF_SYNTHETIC ) ? synthetic_stats : "") +
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
        "  * ==============================\n" +
        ((chaining) ? "  * chaining enabled" : "  * chaining disabled" ) );

        try {
            if (!chaining) {
                env.disableOperatorChaining();
            }

            // run the topology
            //LOG.info("Executing " + IntervalJoinConstants.DEFAULT_TOPO_NAME + " topology");
            JobExecutionResult result = env.execute();
            //LOG.info("Exiting");

            // measure throughput
            double throughput = (double) (ThroughputCounter.getValue() / result.getNetRuntime(TimeUnit.SECONDS));
            LOG.info("Measured throughput: " + throughput + " tuples/second");
            dumpThroughput((int)throughput);

            //LOG.info("Dumping metrics");
            MetricGroup.dumpAll();
        }
        catch (Exception e) {
            LOG.error(e.toString());
        }

    }

    private static void dumpThroughput(int throughput) throws JsonProcessingException, IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode throughputNode = objectMapper.convertValue(throughput, JsonNode.class);
        Util.appendJson(throughputNode, "throughput.json");
    }

    private static int[] ToIntArray(String[] stringArray) {
        return Stream.of(stringArray).mapToInt(Integer::parseInt).toArray();
    }

    private static ArrayList<SourceEvent> parseDataset(String _file_path, String splitter) {
        ArrayList<SourceEvent> ds = new ArrayList<>();
        try {
            Scanner scan = new Scanner(new File(_file_path));
            if (type == testType.UNIFORM_SYNTHETIC || type == testType.ZIPF_SYNTHETIC) {
                if (scan.hasNextLine()) {
                    String par_line = scan.nextLine();
                    String[] params = par_line.split(splitter);
                    if (params.length != 2) {
                        LOG.info("Error in parsing Syntethic parameters");
                        System.exit(1);
                    }
                    numKeys = Integer.valueOf(params[0]);
                    dataSize = Integer.valueOf(params[1]);
                }
            }
            while (scan.hasNextLine()) {
                String line = scan.nextLine();
                if (line.isBlank()) { continue; }
                SourceEvent tuple = new SourceEvent();

                String[] fields = line.split(splitter); // regex quantifier (matches one or many split char)
                switch (type) {
                    case UNIFORM_SYNTHETIC:
                    case ZIPF_SYNTHETIC:
                        if (fields.length != 2) {
                            LOG.info("Error in parsing Syntethic tuple");
                            System.exit(1);
                        }
                        tuple.f0 = Integer.valueOf(fields[0]); // Key
                        tuple.f1 = 5;                          // Value
                        tuple.f2 = Long.valueOf(fields[1]);    // Timestamp
                        break;
                    case ROVIO_TEST:
                        if (fields.length != 4) {
                            LOG.info("Error in parsing tuple");
                            System.exit(1);
                        }
                        tuple.f0 = Integer.valueOf(fields[0]);  // Key
                        tuple.f1 = Integer.valueOf(fields[2]);  // Value
                        tuple.f2 = 0L;                          // Timestamp
                        break;
                    case STOCK_TEST:
                        if (fields.length != 2) {
                            LOG.info("Error in parsing tuple");
                            System.exit(1);
                        }
                        tuple.f0 = Integer.valueOf(fields[0]);  // Key
                        tuple.f1 = Integer.valueOf(fields[1]);  // Value
                        tuple.f2 = 0L;                          // Timestamp
                        break;
                }
                ds.add(tuple);
            }
            dataSize = ds.size();
            scan.close();
            scan = null;
        }
        catch (FileNotFoundException | NullPointerException e) {
            LOG.error("The file {} does not exists", _file_path);
            throw new RuntimeException("The file '"  + _file_path + "' does not exists");
        }
        return ds;
    }

    private static class DataKeySelector implements KeySelector<SourceEvent, Integer> {
        @Override
        public Integer getKey(SourceEvent value) {
            return value.f0;
        }
    }

    private static class IngestionTimeWatermarkStrategy implements WatermarkStrategy<SourceEvent> {

        private IngestionTimeWatermarkStrategy() {}

        public static IngestionTimeWatermarkStrategy create() {
            return new IngestionTimeWatermarkStrategy();
        }

        @Override
        public WatermarkGenerator<SourceEvent> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<SourceEvent> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> timestamp;
        }
    }

}
