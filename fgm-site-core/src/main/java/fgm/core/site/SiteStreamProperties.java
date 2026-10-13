package fgm.core.site;

import fgm.common.cfg.BaseConfiguration;
import fgm.factory.specs.IFgmFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.Properties;

public final class SiteStreamProperties {

    private SiteStreamProperties() {
    }

    public static Properties build(IFgmFactory<?> factory, String nodeId, BaseConfiguration baseConfiguration) throws Exception {
        Properties props = new Properties();

        props.put(StreamsConfig.CLIENT_ID_CONFIG, nodeId);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, nodeId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, baseConfiguration.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, factory.getTimestampExtractor());
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        // --- PRODUCER TUNING (Patient Network, Fast Thread) ---
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), "100");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), "32768");

        // Increased to 60 seconds (default) to survive GC pauses and metadata fetches
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), "60000");

        // Give the network time to survive broker GC pauses
        props.put(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "300000");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG), "10");

        // --- CONSUMER TUNING ---

        // CHANGE 1: cap how many records a single poll() returns.
        // Default for Streams is 1000; with full topics that is the difference between
        // a 50ms poll cycle and a 30s poll cycle.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "50");

        // CHANGE 2: cap fetch sizes so 15-17 simultaneous cold-start consumers do not
        // saturate the brokers. Default fetch.max.bytes is 50MB per consumer.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                String.valueOf(2 * 1024 * 1024));        // 2 MB total per fetch
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                String.valueOf(512 * 1024));             // 512 KB per partition
        // Optional: stop the broker from waiting too long to fill those smaller buffers
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MIN_BYTES_CONFIG), "1");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), "500");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), "300000");

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), "1800000");

        // CHANGE 3: raise session timeout so a brief coordinator pause under load
        // does not evict the consumer. Heartbeat stays at ~1/3 of session timeout.
        // NOTE: the broker setting group.max.session.timeout.ms (default 300000)
        // must be >= this value, otherwise the broker will reject the join.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "180000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "60000");

        // Be more patient about reconnecting to a flapping coordinator
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG), "1000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG), "30000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG), "5000");

        // Removed the String quotes. It is now properly typed as an Integer!
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);

        // Consumer request timeout MUST be greater than session timeout in Kafka 2.x
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), "120000");

        props.put(StreamsConfig.adminClientPrefix("default.api.timeout.ms"), "180000");
        props.put(StreamsConfig.adminClientPrefix(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG), "60000");
        props.put(StreamsConfig.adminClientPrefix(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG), "500");
        props.put(StreamsConfig.adminClientPrefix(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG), "1000");
        props.put(StreamsConfig.adminClientPrefix(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG), "30000");


        return props;
    }

    public static int partitionsOf(String topic, BaseConfiguration baseConfiguration) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,  baseConfiguration.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            TopicDescription td = adminClient.describeTopics(Collections.singletonList(topic)).values().get(topic).get();
            return td.partitions().size();
        }
    }
}