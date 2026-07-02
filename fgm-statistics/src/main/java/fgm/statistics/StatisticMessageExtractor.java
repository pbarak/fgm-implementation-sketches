package fgm.statistics;

import fgm.common.messages.StatisticsMessage;
import fgm.worker.metrictypes.StatisticsMessageType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class StatisticMessageExtractor implements TimestampExtractor {
    private static final String PHI_MESSAGE = StatisticsMessageType.PhiMessage.toString();
    private static final String DRIFT_MESSAGE = StatisticsMessageType.DriftMessage.toString();
    private static final String STREAM_RECORD = StatisticsMessageType.StreamRecordForStatistics.toString();
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord,
                        long partitionTime) {

        boolean phiMessageArrived = ((StatisticsMessage) consumerRecord.value()).getMessageType().equals(PHI_MESSAGE);
        boolean driftMessageArrived = ((StatisticsMessage) consumerRecord.value()).getMessageType().equals(DRIFT_MESSAGE);
        boolean wcRecordArrived = ((StatisticsMessage) consumerRecord.value()).getMessageType().equals(STREAM_RECORD);

        if (phiMessageArrived) {
            return partitionTime;
        } else if (driftMessageArrived) {
            return partitionTime;
        } else if (wcRecordArrived) {
            return (((StatisticsMessage) consumerRecord.value()).getRecord()).getTimeStamp();
        }
        return partitionTime;
    }
}

