package fgm.timestampextractor;

import fgm.streamdata.implementation.stream.WorldCupRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class WorldCupExtractor implements TimestampExtractor {
    @Override
    public long extract (ConsumerRecord<Object, Object> consumerRecord,
                         long partitionTime) {
        WorldCupRecord worldCupRecord = (WorldCupRecord) consumerRecord.key();
        return worldCupRecord.getTimeStamp();
    }
}
