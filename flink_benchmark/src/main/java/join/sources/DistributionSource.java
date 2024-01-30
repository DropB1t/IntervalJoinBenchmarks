package join.sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.XORShiftRandom;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import join.Source_Event;
import util.Sampler;
import util.ThroughputCounter;

import java.util.Random;

public class DistributionSource extends RichParallelSourceFunction<Source_Event> {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionSource.class);
    
    static final int VALUE = 10;

    private long t_start;
    private long t_end;

    private int[] keys;
    private int data_size;
    
    private final long runtime;
    private boolean running = true;
    
    private final int gen_rate;
    private long nt_execution;
    private long generated;
    private int index;
    
    private Sampler throughput_sampler;

    private long ts = 1704106800000L; // January 1, 2024 12:00:00 AM in ms
    private final int offset_seed;
    private Random offset;

    public DistributionSource(long _runtime, int _gen_rate, int _offset_seed, int[] _keys, int _data_size) {
        this.runtime = (long) (_runtime * 1e9); // ns
        this.offset_seed = _offset_seed;
        this.gen_rate = _gen_rate;

        this.data_size = _data_size;
        this.keys = _keys;

        index = 0;
        generated = 0;
        nt_execution = 0;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        throughput_sampler = new Sampler();
        this.offset = new XORShiftRandom(offset_seed);
    }

    @Override
    public void run(final SourceContext<Source_Event> ctx) throws Exception {
        this.t_start = System.nanoTime();

        // generation loop
        while ((System.nanoTime() - this.t_start < runtime) && running) {
            ts += offset.nextInt(500);
            ctx.collectWithTimestamp(new Source_Event(keys[index], VALUE, System.nanoTime()), ts);
            generated++;
            index++;

            if (gen_rate != 0) { // not full speed
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

            throughput_sampler.add(rate);
            //MetricGroup.add("throughput", throughput);
        }
    }
}
