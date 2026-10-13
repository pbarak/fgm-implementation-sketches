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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import java.time.Duration;
import java.util.Properties;

import fgm.queryfunction.implementation.NormHashMapStateQFunction;

public class HoppingWindow extends HoppingWindowBase {

    private static final String STATE_STORE_NAME = "state-store";
    private static final Long SINGLE_DUMMY_KEY = 0L;
    private static final long STATE_STORE_RETENTION_HOURS = 24;

    private static final long ONE_DAY_RETENTION_IN_MILLIS = 24*60*60*1000L;

    static cfg.CfgRetrieval cfgRetrieval = new cfg.CfgRetrieval();

    private static final long STATE_STORE_SIZE_MINUTES = cfgRetrieval.getHoppingWindowSizeMinutes();;
    private static final long WINDOW_MINUTES = cfgRetrieval.getHoppingWindowSizeMinutes();
    private static final long WINDOW_ADVANCE_SECONDS = cfgRetrieval.getHoppingWindowAdvanceSeconds();
    private static final long WINDOW_SIZE_MILLISECONDS = cfgRetrieval.getHoppingWindowSizeMillis();
    private static final String INPUT_TOPIC = cfgRetrieval.getHoppingWindowInputTopic();
    private static final String OUTPUT_TOPIC = cfgRetrieval.getHoppingWindowOutputTopic();
    private static final String BOOTSTRAP_SERVERS = cfgRetrieval.getBootstrapServers();

    public static void main(String[] args) {

        String appName = (args.length > 0 && !args[0].isEmpty())
                ? args[0]
                : null;

        Properties props = getProperties(appName);

        StreamRecordDoubleKVSerdes keyValueSerde = new StreamRecordDoubleKVSerdes();

        /* ---------- same properties block as before ---------- */

        StreamsBuilder builder = new StreamsBuilder();

        /* 1. Read the original topic: (key ⇒ Long, value ⇒ Double) */
        KStream<StreamRecord, Double> input =
                builder.stream(INPUT_TOPIC, Consumed.with(streamRecordSerde, doubleSerde));

        /* 2. Wrap (k,v) in KeyValue< Long, Double > and force a single dummy key (0L)   */
        KStream<Long, KeyValue<StreamRecord, Double>> keyedAsOne =
                input.map((origKey, freq) -> KeyValue.pair(
                        SINGLE_DUMMY_KEY,                         // constant group key
                        KeyValue.pair(origKey, freq)  // payload keeps the real key
                ));


        WindowBytesStoreSupplier supplier =
                Stores.inMemoryWindowStore(
                        STATE_STORE_NAME,            // store name
                        Duration.ofHours(STATE_STORE_RETENTION_HOURS),     // retention
                        Duration.ofMinutes(STATE_STORE_SIZE_MINUTES),    // windowSize (matches TimeWindows)
                        false                     // retainDuplicates
                );

        Materialized<Long, HashMapState, WindowStore<Bytes, byte[]>> mat =
                Materialized.<Long, HashMapState>as(supplier)
                        .withKeySerde(longSerde)
                        .withValueSerde(mapSerde)
                        .withCachingDisabled()    // optional; caching is on-heap too
                        .withLoggingDisabled();   // disables changelog topic



        /* 3. Group by that constant key and build ONE HashMapState                 */
        KTable<Windowed<Long>, HashMapState> globalTable =
                keyedAsOne
                        .groupByKey(Grouped.with(longSerde, keyValueSerde))  // still one group: key=0L
                        .windowedBy(                                         // <-- NEW
                                TimeWindows.ofSizeWithNoGrace(
                                        Duration.ofMinutes(WINDOW_MINUTES)
                                ).advanceBy(Duration.ofSeconds(WINDOW_ADVANCE_SECONDS))
                        )
                        .aggregate(
                                HashMapState::new,
                                (ignored, kv, state) -> {
                                    state.update(kv.key, kv.value);
                                    return state;
                                },
                                mat
                        ).suppress(Suppressed.untilWindowCloses(
                                Suppressed.BufferConfig.unbounded()
                        ));


        /* 4. Convert the KTable back to a stream of global-norm updates            */
        KStream<Windowed<Long>, AggregateResult> resultStream =
                globalTable  // buffer all updates
                        .mapValues(state -> {
                            double norm  = new NormHashMapStateQFunction<>()
                                    .computeQuery(new FgmGlobalStatistic<>(state));
                            return new AggregateResult(norm);
                        })
                        .toStream();           // key is Windowed<Long>(0L)


        /* 5. Ship it out                                                           */
        resultStream.to(
                OUTPUT_TOPIC,
                Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(Long.class, WINDOW_SIZE_MILLISECONDS), // key serde
                        aggregateResultSerde)                           // value serde
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(
                (Thread t, Throwable e) -> e.printStackTrace()
        );
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties getProperties(String appName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, appName);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,
                ONE_DAY_RETENTION_IN_MILLIS);
        return props;
    }

}


