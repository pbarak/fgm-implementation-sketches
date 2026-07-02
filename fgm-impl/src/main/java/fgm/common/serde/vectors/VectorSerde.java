package fgm.common.serde.vectors;

import fgm.common.messages.DriftVectorMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class VectorSerde implements Serde<DriftVectorMessage> {
    private final DriftVectorMessageSerializer serializer;
    private final DriftVectorMessageDeserializer deserializer;

    public VectorSerde() {
        this.serializer = new DriftVectorMessageSerializer();
        this.deserializer = new DriftVectorMessageDeserializer();
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        //there are no any configurable options
    }

    @Override
    public void close() {
        //it does not manage any resources
    }

    @Override
    public Serializer<DriftVectorMessage> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<DriftVectorMessage> deserializer() {
        return deserializer;
    }


}
