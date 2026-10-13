package fgm.customwindow;

import fgm.configuration.CfgRetrieval;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CustomWindowApplication extends CustomWindowAppBase {

    private static final String SINK_NAME = "output-sink";
    private static final String STREAMS_SHUTDOWN_HOOK = "streams-shutdown-hook";
    private static final CountDownLatch latch = new CountDownLatch(1);
    static CfgRetrieval cfgRetrieval = new CfgRetrieval();


    public static void main(String[] args) {

        String inputTopic = args[0];
        String vectorSourceNodeName = args[1];
        String processorName = args[2];
        String nodeId = args[3];
        String outputTopic = args[4];

        Topology topology = new Topology();

        topology.addSource(
                        Topology.AutoOffsetReset.EARLIEST,      //  reset policy
                        vectorSourceNodeName,                        // source-node name
                        //(rec,pt) ->System.currentTimeMillis(),            // TimestampExtractor
                        streamRecordSerde.deserializer(),       //    key deserializer
                        doubleSerde.deserializer(),             //    value deserializer
                        inputTopic)
                .addProcessor(processorName,
                        WCProcessor::new,
                        vectorSourceNodeName)
                .addSink(SINK_NAME, outputTopic, windowedLongSerde.serializer(), aggregateResultSerde.serializer(), processorName);

        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, nodeId);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, nodeId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, nodeId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfgRetrieval.getBootstrapServers());

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class);


        // Increase max.block.ms to allow longer blocking before throwing TimeoutException
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), String.valueOf(900000)); // e.g., 15 minutes

        // Increase delivery.timeout.ms to allow more time for retries & acknowledgments
        props.put(StreamsConfig.producerPrefix(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), String.valueOf(900000)); // e.g., 15 minutes

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), "900000");   // 15 minutes
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG), "10000");     // 10 s

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "300000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "20000"); // 20 s

        // Add these missing configurations
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "10");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MIN_BYTES_CONFIG), "1");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), "500");

        // Increase commit interval to reduce commit frequency
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000); // 30 seconds

        // Add rebalance timeout
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), "600000"); // 10 minutes

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

// attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(STREAMS_SHUTDOWN_HOOK) {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        System.out.println("Starting FGM Application now");
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

