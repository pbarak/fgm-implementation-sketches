package fgm.statistics;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.StatisticsMessage;
import fgm.common.serde.records.RecordDeserializer;
import fgm.common.serde.statistics.StatisticsMessageDeserializer;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.Properties;

public class StatisticApplication {
    private static final CountDownLatch latch = new CountDownLatch(1);

    private static final String SERDES_KEY = "JsonPOJOClass";
    private static final String SHUTDOWN_HOOK_NAME = "shut-down-hook";
    public static void main(String[] args) {

        IFgmFactory<?> factory = FactoryInjection.createFactory();
        BaseConfiguration baseConfiguration = factory.getBaseCfg();
        Map<String, Object> serdeProps = new HashMap<>();


        final Deserializer<?> recordDeserializer = new RecordDeserializer(factory);
        serdeProps.put(SERDES_KEY, RecordDeserializer.class.getName());
        recordDeserializer.configure(serdeProps, false);

        Serde<String> stringSerde = Serdes.String();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();

        final Deserializer<StatisticsMessage> statisticsMessageDeserializer = new StatisticsMessageDeserializer(factory);
        serdeProps.put(SERDES_KEY, StatisticsMessage.class);
        statisticsMessageDeserializer.configure(serdeProps, false);

        Topology topology = new Topology();

        String statisticsSourceNodeName = args[1];

        String statisticsProcessorName = args[2];

        topology.addSource(statisticsSourceNodeName, stringDeserializer, statisticsMessageDeserializer, args[0])
                .addProcessor(statisticsProcessorName, () -> factory.getStatisticsProcessor(factory), statisticsSourceNodeName);

        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, args[3]);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, args[4]);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, args[5]);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, baseConfiguration.getBootstrapServers());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                StatisticMessageExtractor.class);

	props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);
        kafkaStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            //here you should examine the throwable/exception and perform an appropriate action!
            kafkaStreams.close();
        });

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK_NAME) {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.cleanUp();
            kafkaStreams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
