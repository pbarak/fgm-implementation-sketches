package fgm.population;

import java.net.URL;

import fgm.common.cfg.BaseConfiguration;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import fgm.streamdata.StreamRecord;
import fgm.streamdata.implementation.stream.WorldCupRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.*;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.PartitionInfo;
import fgm.common.serde.records.RecordSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;

public class WCProducer {
    private static final String DELIMITER = " ";
    private static Producer<StreamRecord, Double> producer;
    private static String[] fileNames;
    private static String[] nodeTopics;
    private static final Double CASH_REGISTER_VALUE = 1.0d;

    // Total records sent (across all files) that call sendToTopic()
    private static final AtomicInteger globalRecordCounter = new AtomicInteger(0);

    private static final AtomicInteger rrTopicCounter = new AtomicInteger(0);
    // Flush policy: after init is done, flush every N records
    private static final int FLUSH_EVERY = 1000;
    private static final AtomicInteger postInitCounter = new AtomicInteger(0);

    private static final ConcurrentMap<String, Integer> partitionsCache = new ConcurrentHashMap<>();

    private static int partitionsForTopicCached(String topic) {
        return partitionsCache.computeIfAbsent(topic, WCProducer::partitionsForTopic);
    }

    private static final class RoundRobin {
        private final AtomicInteger counter = new AtomicInteger();
        public int next(int numPartitions) {
            return Math.floorMod(counter.getAndIncrement(), numPartitions);
        }
    }

    private static final ConcurrentMap<String, RoundRobin> rrByTopic = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        IFgmFactory<?> factory = FactoryInjection.createFactory();
        BaseConfiguration baseConfiguration = factory.getBaseCfg();
        String topics = baseConfiguration.getNodesInboundTopics();
        nodeTopics = topics.split("\\s*,\\s*");

        // Get folder name from configuration
        String folderName = baseConfiguration.getTopicsPopulationFileName();

        File folder = null;

        // Try to get folder from classpath resources first
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL folderResource = classLoader.getResource(folderName);

        if (folderResource != null) {
            try {
                folder = new File(folderResource.toURI());
                System.out.println("Using folder from classpath: " + folder.getAbsolutePath());
            } catch (Exception e) {
                System.out.println("ERROR: Failed to convert classpath resource to File: " + e);
                return;
            }
        } else {
            folder = new File(folderName);
            if (!folder.exists()) {
                System.out.println("ERROR: Folder not found in classpath or filesystem: " + folderName);
                return;
            }
            System.out.println("Using folder from filesystem: " + folder.getAbsolutePath());
        }

        if (!folder.isDirectory()) {
            System.out.println("ERROR: Path is not a directory: " + folder.getAbsolutePath());
            return;
        }

        File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt") && !name.startsWith("."));
        if (files == null || files.length == 0) {
            System.out.println("ERROR: No .txt files found in folder: " + folder.getAbsolutePath());
            return;
        }

        fileNames = new String[files.length];
        for (int i = 0; i < files.length; i++) {
            fileNames[i] = files[i].getAbsolutePath();
        }

        Arrays.sort(fileNames, new Comparator<String>() {
            @Override
            public int compare(String f1, String f2) {
                int num1 = extractNumericSuffix(f1);
                int num2 = extractNumericSuffix(f2);
                return Integer.compare(num1, num2);
            }

            private int extractNumericSuffix(String filename) {
                try {
                    int lastUnderscore = filename.lastIndexOf('_');
                    int dotIndex = filename.lastIndexOf(".txt");
                    if (lastUnderscore != -1 && dotIndex != -1) {
                        String numStr = filename.substring(lastUnderscore + 1, dotIndex);
                        return Integer.parseInt(numStr);
                    }
                } catch (Exception e) {
                    return Integer.MAX_VALUE;
                }
                return Integer.MAX_VALUE;
            }
        });

        System.out.println("Processing " + fileNames.length + " files in order:");
        for (String fileName : fileNames) {
            System.out.println("  - " + fileName);
        }

        init(baseConfiguration.getBootstrapServers());

        for (String fileName : fileNames) {
            System.out.println("Starting to process file: " + fileName);
            processFile(fileName.trim());
            System.out.println("Finished processing file: " + fileName);
        }

        // close() flushes any remaining buffered records
        producer.close();
        producer = null;
        System.out.println("All files processed. Producer closed.");
    }

    private static void processFile(String filePath) {
        LineNumberReader reader = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                System.out.println("ERROR: File not found: " + filePath);
                return;
            }

            reader = new LineNumberReader(new FileReader(file));

            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                try {
                    String[] tokens = line.split(DELIMITER);
                    if (tokens.length >= 8) {
                        Long timestamp = Long.parseLong(tokens[0]);
                        Long clientID = Long.parseLong(tokens[1]);
                        Long objectID = Long.parseLong(tokens[2]);
                        Long size = Long.parseLong(tokens[3]);
                        Long method = Long.parseLong(tokens[4]);
                        Long status = Long.parseLong(tokens[5]);
                        Long type = Long.parseLong(tokens[6]);
                        Long server = Long.parseLong(tokens[7]);

                        WorldCupRecord wc = new WorldCupRecord(timestamp, clientID, objectID, size, method, status, type, server);
                        sendToTopic(wc);
                        lineCount++;
                    } else {
                        System.out.println("WARNING: Skipping malformed line in " + filePath + ": " + line);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("WARNING: Error parsing line in " + filePath + ": " + line + " - " + e.getMessage());
                }
            }

            System.out.println("Sent end-of-file marker for " + filePath);
            System.out.println("Total records processed from " + filePath + ": " + lineCount);

        } catch (Exception e) {
            System.out.println("ERROR: Error processing file " + filePath + ": " + e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    System.out.println("WARNING: Error closing reader for " + filePath + ": " + e.getMessage());
                }
            }
        }
    }

    private static int partitionsForTopic(String topic) {
        try {
            List<PartitionInfo> infos = producer.partitionsFor(topic);
            int n = (infos == null) ? 0 : infos.size();
            if (n == 0) {
                System.out.println("WARNING: No partition metadata for topic '" + topic + "'. Will fall back to default partitioner");
            }
            return n;
        } catch (Exception e) {
            System.out.println("WARNING: Failed to fetch partitions for topic '" + topic + "': " + e + ". Falling back.");
            return 0;
        }
    }

    private static void maybeFlushAfterInit(int currentRecordNumber, int numberOfTopics) {
        // Only start periodic flush after ALL initial records (one per topic) are sent
        if (currentRecordNumber <= numberOfTopics) return;

        int n = postInitCounter.incrementAndGet();
        if (n % FLUSH_EVERY == 0) {
            producer.flush();
            System.out.println("Flushed after " + n + " post-init records (batch size=" + FLUSH_EVERY + ")");
        }
    }

////    // Simplified sendToTopic in WCProducer.java
////    private static void sendToTopic(WorldCupRecord streamRecord) {
////        int currentRecordNumber = globalRecordCounter.incrementAndGet();
////        int numberOfTopics = nodeTopics.length;
////        int topicIndex = Math.floorMod(rrTopicCounter.getAndIncrement(), numberOfTopics);
////        String topic = nodeTopics[topicIndex];
////
////        // Force partition 0 since your script only creates 1 partition
////        ProducerRecord<StreamRecord, Double> rec = new ProducerRecord<>(
////                topic,
////                0,
////                System.currentTimeMillis(),
////                streamRecord,
////                CASH_REGISTER_VALUE
////        );
////
////        producer.send(rec, (md, ex) -> {
////            if (ex != null) System.out.println("ERROR: Send failed to " + topic + " - " + ex);
////        });
////
////        maybeFlushAfterInit(currentRecordNumber, numberOfTopics);
////    }

    private static void sendToTopic(WorldCupRecord streamRecord) {
        int currentRecordNumber = globalRecordCounter.incrementAndGet();
        int numberOfTopics = nodeTopics.length;

        // Initial phase: first N records go to partition 0 of each topic in order (1 per topic)
        if (currentRecordNumber <= numberOfTopics) {
            String topic = nodeTopics[currentRecordNumber - 1];

            ProducerRecord<StreamRecord, Double> rec = new ProducerRecord<>(
                    topic,
                    0,
                    System.currentTimeMillis(),
                    streamRecord,
                    CASH_REGISTER_VALUE
            );

            producer.send(rec, (md, ex) -> {
                if (ex != null) {
                    System.out.println("ERROR: Send failed (initial) - " + ex);
                } else {
                    System.out.println("Sent initial record #" + currentRecordNumber + " to topic " + topic + " partition 0");
                }
            });

            // IMPORTANT: flush ONLY after the last initial record is sent
            if (currentRecordNumber == numberOfTopics) {
                producer.flush();
                System.out.println("Initial records delivered for all topics -> flushed once (init complete).");
            }
            return;
        }

        // Normal phase: round-robin across topics
        int topicIndex = Math.floorMod(rrTopicCounter.getAndIncrement(), numberOfTopics);
        String topic = nodeTopics[topicIndex];

        int numPartitions = partitionsForTopicCached(topic);

        if (numPartitions <= 0) {
            ProducerRecord<StreamRecord, Double> rec = new ProducerRecord<>(
                    topic, null, System.currentTimeMillis(), streamRecord, CASH_REGISTER_VALUE
            );
            producer.send(rec, (md, ex) -> {
                if (ex != null) System.out.println("ERROR: Send failed - " + ex);
            });

            maybeFlushAfterInit(currentRecordNumber, numberOfTopics);
            return;
        }

        int partition;
        if (numPartitions == 1) {
            partition = 0;
        } else {
            int availablePartitions = numPartitions - 1;
            int roundRobinIndex = rrByTopic.computeIfAbsent(topic, t -> new RoundRobin()).next(availablePartitions);
            partition = roundRobinIndex + 1; // partitions 1..(numPartitions-1)
        }

        ProducerRecord<StreamRecord, Double> rec =
                new ProducerRecord<>(topic, partition, System.currentTimeMillis(), streamRecord, CASH_REGISTER_VALUE);

        producer.send(rec, (md, ex) -> {
            if (ex != null) System.out.println("ERROR: Send failed - " + ex);
        });

        maybeFlushAfterInit(currentRecordNumber, numberOfTopics);
    }

    private static void init(String bootstrapServers) {
        if (producer == null) {
            System.out.println("Initializing the producer");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrapServers);
            properties.put("key.serializer", RecordSerializer.class.getName());
            properties.put("value.serializer", DoubleSerializer.class.getName());
            producer = new KafkaProducer<>(properties);
        }
    }
}
