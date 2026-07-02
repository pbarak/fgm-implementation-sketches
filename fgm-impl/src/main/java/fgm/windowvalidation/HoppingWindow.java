package fgm.windowvalidation;

import fgm.common.statevector.HashMapState;
import fgm.coordinator.collectorsinterfaces.implementation.processors.state.FgmGlobalStatistic;
import fgm.streamdata.StreamRecord;
import fgm.windowvalidation.result.AggregateResult;
import fgm.windowvalidation.serde.StreamRecordDoubleKVSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import java.util.Properties;

import fgm.queryfunction.implementation.NormHashMapStateQFunction;

public class HoppingWindow extends HoppingWindowBase {

    private static final String NAME_CONFIG = "fgm.windowvalidation";
    private static final String STATE_STORE_NAME = "state-store";
    private static final long WINDOW_SIZE_MILLISECONDS = 60000L; // 1 Minute
    private static final long WINDOW_ADVANCE_MILLISECONDS = 5000L; // 5 Seconds
    private static final long RETENTION_MILLISECONDS = 24 * 60 * 60 * 1000L; // 24 Hours
    private static final String INPUT_TOPIC = "streamrecords-input";
    private static final String OUTPUT_TOPIC = "query-result-output";

    public static void main(String[] args) {
        Properties props = getProperties();

        StreamRecordDoubleKVSerdes keyValueSerde = new StreamRecordDoubleKVSerdes();

        StreamsBuilder builder = new StreamsBuilder();

        /* 1. Read the original topic */
        KStream<StreamRecord, Double> input =
                builder.stream(INPUT_TOPIC, Consumed.with(streamRecordSerde, doubleSerde));

        /* 2. Wrap (k,v) in KeyValue< Long, Double > and force a single dummy key (0L) */
        KStream<Long, KeyValue<StreamRecord, Double>> keyedAsOne =
                input.map((origKey, freq) -> KeyValue.pair(
                        0L,
                        new KeyValue<>(origKey, freq)
                ));

        /* 2.0.1 API: Use persistentWindowStore and long milliseconds (3 segments is the standard default) */
        WindowBytesStoreSupplier supplier =
                Stores.persistentWindowStore(
                        STATE_STORE_NAME,
                        RETENTION_MILLISECONDS,
                        3,
                        WINDOW_SIZE_MILLISECONDS,
                        false
                );

        Materialized<Long, HashMapState, WindowStore<Bytes, byte[]>> mat =
                Materialized.<Long, HashMapState>as(supplier)
                        .withKeySerde(longSerde)
                        .withValueSerde(mapSerde)
                        .withCachingDisabled()
                        .withLoggingDisabled();

        /* 3. Group by that constant key and build ONE HashMapState */
        KTable<Windowed<Long>, HashMapState> globalTable =
                keyedAsOne
                        // 2.0.1 API: Use Serialized instead of Grouped
                        .groupByKey(Serialized.with(longSerde, keyValueSerde))
                        // 2.0.1 API: Use TimeWindows.of() with explicit longs
                        .windowedBy(TimeWindows.of(WINDOW_SIZE_MILLISECONDS).advanceBy(WINDOW_ADVANCE_MILLISECONDS))
                        .aggregate(
                                // Explicit lambda fixes the generic inference errors for HashMapState
                                () -> new HashMapState(),
                                (Long ignored, KeyValue<StreamRecord, Double> kv, HashMapState state) -> {
                                    state.update(kv.key, kv.value);
                                    return state;
                                },
                                mat
                        );
        // The .suppress() method has been removed entirely as it does not exist in 2.0.1

        /* 4. Convert the KTable back to a stream of global-norm updates */
        KStream<Windowed<Long>, AggregateResult> resultStream =
                globalTable
                        .toStream()
                        .mapValues(state -> {
                            double norm  = new NormHashMapStateQFunction<>()
                                    .computeQuery(new FgmGlobalStatistic<>(state));
                            return new AggregateResult(norm);
                        });

        /* 5. Ship it out */
        resultStream.to(
                OUTPUT_TOPIC,
                // 2.0.1 API: timeWindowedSerdeFrom only takes 1 argument
                Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class), aggregateResultSerde)
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(
                (Thread t, Throwable e) -> e.printStackTrace()
        );
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, NAME_CONFIG);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, NAME_CONFIG);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, NAME_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        // Note: EXACTLY_ONCE is supported in 2.0.1, but AT_LEAST_ONCE is safer if brokers are unstable
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        return props;
    }
}