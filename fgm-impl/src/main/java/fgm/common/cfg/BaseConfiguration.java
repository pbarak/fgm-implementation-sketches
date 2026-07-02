package fgm.common.cfg;


import org.apache.kafka.streams.processor.PunctuationType;

public interface BaseConfiguration {
    String getBootstrapServers();

    String getRmiHostName();

    String getIncrementTopic();

    String getIncrementTopicKey();

    String getPhiMessagesTopic();

    String getPhiMessagesTopicKey();

    String getDriftMessagesTopic();

    String getDriftMessagesTopicKey();

    String getCoordinatorLogTopic();

    String getWorkerLogTopic();

    String getWorkerLogsUpdatesTopic();

    String getWorkerIncrementSinkName();

    String getWorkerStatisticEventsSinkName();

    String getWorkerLogsUpdatesSinkName();

    String getWorkerLogsSinkName();

    String getStatisticEventsTopic();

    Boolean hasWindow();

    String getStateUpdateStatisticsKey();

    String getRemoteStoreBindName();

    Integer getRemoteStorePort();

    Integer getNumberOfNodes();

    String getNodesInboundTopics();

    Double getLowerThreshold();

    double getUpperThreshold();

    String getTopicsPopulationFileName();

    Double ePsi();

    String getCoordinatorStateStoreName();

    Integer getWindowSecondsWidth();

    Long getSlideSizeMillis();

    int getMultiPartitionPunctuationMillis();

    PunctuationType getPunctuationType();

    PunctuationType getMultiPartitionPunctuationType();

    Boolean isWarmUpEnabled();

}
