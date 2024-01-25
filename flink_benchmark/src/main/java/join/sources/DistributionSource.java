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
    
    static final int VALUE_MAX = 10;

    private long t_start;
    private long t_end;

    private int[] keys;
    
    private boolean running = true;
    //private final long runtime;
    
    private int generated = 0;
    private final int gen_rate;
    
    private Sampler throughput_sampler;
    private final int throughput;

    private long ts = 1704106800000L; // January 1, 2024 12:00:00 AM in ms

    private Random rnd;
    private Random offset;
    private final int offset_seed;

    public DistributionSource(long _runtime, int _gen_rate, int _throughput, int _offset_seed, int[] _keys) {
        //this.runtime = (long) (_runtime * 1e9); // ns
        this.offset_seed = _offset_seed;
        this.throughput = _throughput;
        this.gen_rate = _gen_rate;
        this.keys = _keys;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.rnd = new XORShiftRandom();
        this.offset = new XORShiftRandom(offset_seed);
        throughput_sampler = new Sampler();
    }

    @Override
    public void run(final SourceContext<Source_Event> ctx) throws Exception {
        this.t_start = System.nanoTime();

        // generation loop
        while (/* (System.nanoTime() - this.t_start < runtime) */ generated < throughput && running) {
            ts += offset.nextInt(500);
            ctx.collectWithTimestamp(new Source_Event(keys[generated], rnd.nextInt(VALUE_MAX) + 1, System.nanoTime()), ts);
            generated++;

            if (gen_rate != 0) { // not full speed
                long delay_nsec = (long) ((1.0d / gen_rate) * 1e9);
                active_delay(delay_nsec);
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
        }
        else {
            long t_elapsed = (this.t_end - this.t_start) / 1000000; // elapsed time in milliseconds
            double rate = Math.floor( generated / ((this.t_end - this.t_start) / 1e9) ); // per second
            LOG.info("[Source] execution time: " + t_elapsed +
                    " ms, generated: " + generated +
                    ", bandwidth: " + rate +  // tuples per second
                    " tuples/s");

            throughput_sampler.add(rate);
            //MetricGroup.add("throughput", throughput);
        }
    }
}
