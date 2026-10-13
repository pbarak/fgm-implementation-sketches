package fgm.windowvalidation.consumer;

import fgm.common.cfg.implementation.FilePropertiesLocalStore;
import fgm.common.serde.windowaggregate.AggregateResultSerde;
import fgm.windowvalidation.result.AggregateResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * A Processor API application that consumes the windowed AggregateResult messages
 * produced by the DSL example (WindowsExperimentsNew) on the "query-result-output" topic,
 * and writes each record to a text file with immediate flush.
 */
public class WindowConsumer {

    static cfg.CfgRetrieval cfgRetrieval = new cfg.CfgRetrieval();

    static final String INPUT_TOPIC = cfgRetrieval.getWindowConsumerInputTopic();
    static final String BOOTSTRAP_SERVERS = cfgRetrieval.getBootstrapServers();
    // Window size matches the DSL's 3-minute windows
    static final long WINDOW_SIZE_MS = cfgRetrieval.getWindowConsumerWidthMillis();
    // Output file path
    static final String OUTPUT_FILE = cfgRetrieval.getWindowConsumerOutputFile();


    static final String SOURCE_NAME = "Source";
    static final String PROCESSOR_NAME = "FileWriterProcessor";


    public static void main(String[] args) {
        String application_id = args[0];
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, application_id);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, application_id);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, application_id);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // Disable caching for immediate write-through
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // Create the windowed key serde for Long
        final Serde<Windowed<Long>> windowedKeySerde =
                WindowedSerdes.timeWindowedSerdeFrom(Long.class, WINDOW_SIZE_MS);
        // Create the value serde for AggregateResult
        Serde<AggregateResult> valueSerde = new AggregateResultSerde();


        // Build topology
        Topology topology = new Topology();

        // 1. Source: read from OUTPUT_TOPIC with windowed key and AggregateResult value
        topology.addSource(Topology.AutoOffsetReset.EARLIEST,
                SOURCE_NAME,
                (rec, pt) -> rec.timestamp(),
                windowedKeySerde.deserializer(),
                valueSerde.deserializer(),
                INPUT_TOPIC
        );

        // 2. Processor: print and write each record to file
        topology.addProcessor(
                PROCESSOR_NAME,
                new ProcessorSupplier<Windowed<Long>, AggregateResult>() {
                    @Override
                    public Processor<Windowed<Long>, AggregateResult> get() {
                        return new FileWritingProcessor();
                    }
                },
                SOURCE_NAME
        );

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler((t, e) -> e.printStackTrace());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * Processor that writes each consumed window record to a text file and flushes.
     */
    public static class FileWritingProcessor extends AbstractProcessor<Windowed<Long>, AggregateResult> {
        private BufferedWriter writer;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            try {
                writer = new BufferedWriter(new FileWriter(OUTPUT_FILE, true)); // append mode
            } catch (IOException e) {
                throw new RuntimeException("Failed to open output file", e);
            }
        }

        @Override
        public void process(Windowed<Long> key, AggregateResult value) {
            long start = key.window().start();
            long end = key.window().end();
            String line = String.format(
                    "Window [%d - %d], key=%d, norm=%.5f",
                    start, end, key.key(), value.getQueryResult()
            );
            System.out.println(line);
            try {
                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                // Log and continue
                System.err.println("Failed to write record to file: " + e.getMessage());
            }
        }

        @Override
        public void close() {
            try {
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }
}