package fgm.common.cfg.implementation;

import fgm.common.cfg.BaseConfiguration;
import org.apache.kafka.streams.processor.PunctuationType;

public class BaseCfgImpl implements BaseConfiguration {
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String JAVA_RMI_SERVER_HOSTNAME = "java.rmi.server.hostname";
    private static final String INCREMENT_TOPIC = "increment.messages.coordinator.topic";
    private static final String INCREMENT_TOPIC_KEY = "fgm.increment.messages.topic.key";
    private static final String PHI_MESSAGES_TOPIC = "phi.messages.coordinator.topic";
    private static final String PHI_MESSAGES_TOPIC_KEY = "fgm.phimessages.topic.key";
    private static final String DRIFT_MESSAGES_TOPIC = "xi.messages.coordinator.topic";
    private static final String DRIFT_MESSAGES_TOPIC_KEY = "fgm.ximessages.topic.key";
    private static final String STATISTIC_MESSAGES = "statistics.events.messages.topic";
    private static final String COORDINATOR_LOG_TOPIC = "coordinator.log.topic";
    private static final String WORKER_LOG_TOPIC = "worker.logs.topic";
    private static final String WORKER_LOG_UPDATE_TOPIC = "worker.logs.updates.topic";
    private static final String WORKER_INCREMENTS_SINK_NAME = "worker.increment.sink.name";
    private static final String WORKER_STATISTICS_UPDATES_SINK_NAME = "worker.statistics.updates.sink.name";
    private static final String WORKER_LOGS_SINK_NAME = "worker.logs.sink.name";
    private static final String WORKER_LOGS_UPDATES_SINK_NAME = "worker.logs.updates.sink.name";
    private static final String STATISTICS_WORLDCUP_RECORDS_KEY = "statistics.worldcup.records.messages.key";
    private static final String REMOTESTORE_BIND_NAME = "remotestore.bind.name";
    private static final String REMOTESTORE_PORT = "remotestore.port";
    private static final String NUMBER_OF_NODES = "numberofnodes";
    private static final String NODE_TOPICS = "node.topics";
    private static final String LOWER_BOUND_OUTLIER = "lower.bound.outlier";
    private static final String UPPER_BOUND_OUTLIER = "upper.bound.outlier";
    private static final String INBOUND_FILENAME = "world.cup.filename";
    private static final String E_PSI = "algorithm.ePsi";
    private static final String WINDOW_SECONDS_WIDTH = "window.width";
    private static final String COORDINATOR_STATE_STORE_NAME = "coordinator.state.store.name";
    private static final String HAS_WINDOW = "has.window";
    private static final String SLIDE_SIZE_MILLIS = "sliding.window.width.milliseconds";
    private static final String MULTI_PARTITION_PUNCTUATION_MILLIS = "multi.partition.punctuation.milliseconds";
    private static final String PUNCTUATION_TYPE = "window.punctuation.type";
    private static final String WARMUP_ENABLED = "warmup.enabled";
    FilePropertiesLocalStore filePropertiesLocalStore;

    public BaseCfgImpl(FilePropertiesLocalStore filePropertiesLocalStore) {
        if (filePropertiesLocalStore == null) {
            this.filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();
        }
    }

    public Boolean hasWindow() {
        String hasWindowStr = filePropertiesLocalStore.retrieveString(HAS_WINDOW);

        // First, check if the retrieved value is null or empty.
        if (hasWindowStr == null || hasWindowStr.trim().isEmpty()) {
            throw new IllegalStateException("Required property 'HAS_WINDOW' is missing or empty.");
        }

        // For strict validation (only "true" or "false" are allowed)
        if (hasWindowStr.trim().equalsIgnoreCase("true")) {
            return true;
        }
        if (hasWindowStr.trim().equalsIgnoreCase("false")) {
            return false;
        }

        // If the value is neither "true" nor "false", it's invalid.
        throw new IllegalArgumentException("Invalid boolean value for HAS_WINDOW: " + hasWindowStr);
    }

    public BaseCfgImpl() {
        this.filePropertiesLocalStore = FilePropertiesLocalStore.getInstance();
    }

    public FilePropertiesLocalStore getFilePropertiesLocalStore() {
        return filePropertiesLocalStore;
    }

    public String getBootstrapServers() {
        return filePropertiesLocalStore.retrieveString(BOOTSTRAP_SERVERS);
    }

    public String getRmiHostName() {
        return filePropertiesLocalStore.retrieveString(JAVA_RMI_SERVER_HOSTNAME);
    }

    public String getIncrementTopic() {
        return filePropertiesLocalStore.retrieveString(INCREMENT_TOPIC);
    }

    public String getIncrementTopicKey() {
        return filePropertiesLocalStore.retrieveString(INCREMENT_TOPIC_KEY);
    }

    public String getPhiMessagesTopic() {
        return filePropertiesLocalStore.retrieveString(PHI_MESSAGES_TOPIC);
    }

    public String getPhiMessagesTopicKey() {
        return filePropertiesLocalStore.retrieveString(PHI_MESSAGES_TOPIC_KEY);
    }

    public String getDriftMessagesTopic() {
        return filePropertiesLocalStore.retrieveString(DRIFT_MESSAGES_TOPIC);
    }

    public String getDriftMessagesTopicKey() {
        return filePropertiesLocalStore.retrieveString(DRIFT_MESSAGES_TOPIC_KEY);
    }

    public String getCoordinatorLogTopic() {
        return filePropertiesLocalStore.retrieveString(COORDINATOR_LOG_TOPIC);
    }

    public String getWorkerLogTopic() {
        return filePropertiesLocalStore.retrieveString(WORKER_LOG_TOPIC);
    }

    public String getWorkerLogsUpdatesTopic() {
        return this.filePropertiesLocalStore.retrieveString(WORKER_LOG_UPDATE_TOPIC);
    }

    public String getWorkerIncrementSinkName() {
        return filePropertiesLocalStore.retrieveString(WORKER_INCREMENTS_SINK_NAME);
    }

    public String getWorkerStatisticEventsSinkName() {
        return filePropertiesLocalStore.retrieveString(WORKER_STATISTICS_UPDATES_SINK_NAME);
    }

    public String getWorkerLogsSinkName() {
        return filePropertiesLocalStore.retrieveString(WORKER_LOGS_SINK_NAME);
    }

    public String getWorkerLogsUpdatesSinkName() {
        return filePropertiesLocalStore.retrieveString(WORKER_LOGS_UPDATES_SINK_NAME);
    }

    public String getStatisticEventsTopic() {
        return filePropertiesLocalStore.retrieveString(STATISTIC_MESSAGES);
    }

    public String getStateUpdateStatisticsKey() {
        return filePropertiesLocalStore.retrieveString(STATISTICS_WORLDCUP_RECORDS_KEY);
    }

    public String getRemoteStoreBindName() {
        return filePropertiesLocalStore.retrieveString(REMOTESTORE_BIND_NAME);
    }

    public Integer getRemoteStorePort() {
        return filePropertiesLocalStore.retrieveInt(REMOTESTORE_PORT);
    }

    public Integer getNumberOfNodes() {
        return filePropertiesLocalStore.retrieveInt(NUMBER_OF_NODES);
    }

    public String getNodesInboundTopics() {
        return filePropertiesLocalStore.retrieveString(NODE_TOPICS).trim();
    }

    public Double getLowerThreshold() {
        return filePropertiesLocalStore.retrieveDouble(LOWER_BOUND_OUTLIER);
    }

    public double getUpperThreshold() {
        return filePropertiesLocalStore.retrieveDouble(UPPER_BOUND_OUTLIER);
    }

    public String getTopicsPopulationFileName() {
        return filePropertiesLocalStore.retrieveString(INBOUND_FILENAME);
    }

    public Double ePsi() {
        return filePropertiesLocalStore.retrieveDouble(E_PSI);
    }

    public String getCoordinatorStateStoreName() {
        return filePropertiesLocalStore.retrieveString(COORDINATOR_STATE_STORE_NAME);
    }

    public Integer getWindowSecondsWidth() {
        return filePropertiesLocalStore.retrieveInt(WINDOW_SECONDS_WIDTH);
    }


    public Long getSlideSizeMillis() {
        return filePropertiesLocalStore.retrieveInt(SLIDE_SIZE_MILLIS).longValue();
    }

    public  int getMultiPartitionPunctuationMillis() {
       return filePropertiesLocalStore.retrieveInt(MULTI_PARTITION_PUNCTUATION_MILLIS);
    }

    public PunctuationType getPunctuationType() {
        String punctuationType = filePropertiesLocalStore.retrieveString(PUNCTUATION_TYPE);
        return PunctuationType.valueOf(punctuationType);
    }

    public PunctuationType getMultiPartitionPunctuationType() {
        return PunctuationType.WALL_CLOCK_TIME;
    }

    public Boolean isWarmUpEnabled() {
        Object raw = filePropertiesLocalStore.retrieveProperty(WARMUP_ENABLED);
        if (raw == null || raw.toString().trim().isEmpty()) {
            return true; // default: warm-up on, preserves current behavior
        }
        String v = raw.toString().trim();
        if (v.equalsIgnoreCase("true")) {
            return true;
        }
        if (v.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value for warmup.enabled: " + v);
    }
}
