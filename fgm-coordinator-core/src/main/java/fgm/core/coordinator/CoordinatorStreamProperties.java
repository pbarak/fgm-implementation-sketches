package fgm.core.coordinator;

import fgm.common.cfg.BaseConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Factory for Kafka Streams configuration properties used by the CoordinatorApplication.
 */
public final class CoordinatorStreamProperties {

    private CoordinatorStreamProperties() {
    }

    public static Properties build(BaseConfiguration baseConfiguration, String applicationId) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, baseConfiguration.getBootstrapServers());

        // --- PRODUCER SETTINGS ---
        // Increased to 60 seconds (default) so the thread doesn't crash on minor network/broker hiccups
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), "60000");

        // --- CONSUMER SETTINGS ---

        // CHANGE 1: cap how many records a single poll() returns.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "50");

        // CHANGE 2: cap fetch sizes so the coordinator does not bulk-pull either.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG),
                String.valueOf(2 * 1024 * 1024));        // 2 MB total per fetch
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG),
                String.valueOf(512 * 1024));             // 512 KB per partition
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MIN_BYTES_CONFIG), "1");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG), "500");

        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG), "1800000"); // 30 min
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG), "300000");   // 5 min
	props.put(StreamsConfig.consumerPrefix(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), "300000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG), "10000");      // 10 s

        // --- NETWORK SURVIVAL ---

        // CHANGE 3: raise session timeout so a brief coordinator pause under load
        // does not evict the consumer. Heartbeat stays at ~1/3 of session timeout.
        // NOTE: broker group.max.session.timeout.ms (default 300000) must be >= this value.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "180000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "30000");

        // Be more patient about reconnecting to a flapping coordinator
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG), "1000");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG), "30000");

        // --- STREAMS-SPECIFIC TUNING ---
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");

        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 30000);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        return props;
    }
}