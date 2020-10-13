package com.gonnect.hazelcast.jet.peipeline;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.WindowDefinition;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Showcases the Sliding Window Aggregation operator of the Pipeline API.
 * <p>
 * The sample starts a thread that randomly generates trade events and
 * puts them into a Hazelcast Map. The Jet job receives these events and
 * calculates for each stock the number of trades completed within the
 * duration of a sliding window. It outputs the results to the console
 * log.
 */
public class PipelineExchange {

    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 3_000;
    private static final int SLIDE_STEP_MILLIS = 500;
    private static final int TRADES_PER_SEC = 3_000;
    private static final int NUMBER_OF_TICKERS = 10;
    private static final int JOB_DURATION = 15;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.readFrom(EventSource.tradeStream(NUMBER_OF_TICKERS, TRADES_PER_SEC))
                .withNativeTimestamps(3000)
                .groupingKey(Event::getTicker)
                .window(WindowDefinition.sliding(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS))
                .aggregate(counting())
                .writeTo(Sinks.logger(wr -> String.format("%s %5s %4d", toLocalTime(wr.end()), wr.key(), wr.result())));

        return p;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();
        try {
            Job job = jet.newJob(buildPipeline());
            SECONDS.sleep(JOB_DURATION);
            job.cancel();
            job.join();
        } catch (CancellationException ignored) {
        } finally {
            Jet.shutdownAll();
        }
    }

    private static LocalTime toLocalTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalTime();
    }
}
