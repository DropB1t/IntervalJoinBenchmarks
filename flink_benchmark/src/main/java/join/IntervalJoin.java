package join;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;

import util.Log;

public class IntervalJoin extends ProcessJoinFunction<Event, Event, Event> {

    private static final Logger LOG = Log.get(ConsoleSink.class);
    private long processed;
    private long t_start;
    private long t_end;

    @Override
    public void open(Configuration parameters) throws Exception {
        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples
    }

    @Override
    public void processElement(Event first, Event second, Context ctx, Collector<Event> out) throws Exception {
        //LOG.info(first.key + " | " + second.key);
        Event out_t = new Event();
        out_t.f0 = first.f0;
        out_t.f1 = Integer.valueOf(first.f1+second.f1);
        out_t.f2 = Long.valueOf(Math.max(first.f2, second.f2));
        out.collect(out_t);
        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void close() {
        if (processed == 0) {
            LOG.info("[Join] processed tuples: " + processed);
        }
        else {
            int id = getRuntimeContext().getIndexOfThisSubtask();
            int n = getRuntimeContext().getNumberOfParallelSubtasks();
            long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds
            LOG.info("[Join] " + id + "/" + n + " execution time: " + t_elapsed +
                    " ms, processed: " + processed +
                    ", bandwidth: " + Math.floor(processed / ((double)t_elapsed / 1000)) +  // tuples per second
                    " tuples/s");
        }
    }
}
