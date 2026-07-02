package fgm.customwindow;

import fgm.common.Vector;
import fgm.common.statevector.HashMapState;
import fgm.configuration.CfgRetrieval;
import fgm.streamdata.StreamRecord;
import fgm.queryfunction.implementation.NormHashMapStateQFunction;
import fgm.queryfunction.QueryFunction;
import fgm.worker.windowoperator.implementation.CustomWindow;
import fgm.windowvalidation.result.AggregateResult;

import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.kstream.Windowed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class WCProcessor
        implements Processor<StreamRecord, Double, Windowed<Long>, AggregateResult> {


    private static final CfgRetrieval cfgRetrieval = new CfgRetrieval();
    private static final long WINDOW_SIZE_MS = Duration.ofMillis(cfgRetrieval.getWindowMillisWidth()).toMillis(); // 60_000
    private static final long ADVANCE_MS = Duration.ofMillis(cfgRetrieval.getWindowAdvanceMillis()).toMillis(); //   5_000
    private final Logger logger = LoggerFactory.getLogger(WCProcessor.class);
    private static final long DUMMY_KEY = 0L;

    private ProcessorContext<Windowed<Long>, AggregateResult> context;
    private final CustomWindow<?> window;
    QueryFunction<HashMapState> queryFunction = new NormHashMapStateQFunction<>();

    public WCProcessor() {
        HashMapState hashMapState = new HashMapState();
        this.window = new CustomWindow<>(hashMapState, (int) Duration.ofMillis(cfgRetrieval.getWindowMillisWidth()).getSeconds()); // 1 minute
    }

    @Override
    public void init(final ProcessorContext<Windowed<Long>, AggregateResult> context) {
        this.context = context;
        context.schedule(Duration.ofMillis(ADVANCE_MS),
                PunctuationType.STREAM_TIME,
                this::punctuationFunction);
    }

    @Override
    public void process(final Record<StreamRecord, Double> record) {
        StreamRecord key = record.key();
        Double val = record.value();
        this.window.update(key, val, record.timestamp());
        // NO EMISSION HERE
    }

    @Override
    public void close() {
        // No-op
    }

    //emit only on the CLOSE boundary
    private void punctuationFunction(long punctuationTS) {

        // advance internal sliding-window state
        window.slidePunctuate(punctuationTS);

        AggregateResult aggregateResult = new AggregateResult(getAggregationResult());
        Windowed<Long> windowedKey = buildWindowedKey(punctuationTS); // note: ts == windowEnd
        context.forward(new Record<>(windowedKey, aggregateResult, punctuationTS));
    }

    //compute the same window key that Kafka Streams uses
    private Windowed<Long> buildWindowedKey(long windowEnd) {
        long windowStart = windowEnd - WINDOW_SIZE_MS;          // [start , end)
        TimeWindow tw = new TimeWindow(windowStart, windowEnd);
        return new Windowed<>(DUMMY_KEY, tw);                          // constant dummy key 0L
    }

    private Double getAggregationResult() {
        Vector vector = this.window.getLocalStatistic().getVector();
        HashMapState hashMapState = (HashMapState) vector;
        return this.queryFunction.computeQuery(hashMapState);
    }

}
