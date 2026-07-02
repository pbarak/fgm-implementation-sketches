package fgm.common.serde.vectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.serde.common.CamelCaseNamingStrategy;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class DriftVectorMessageSerializer implements Serializer<DriftVectorMessage> {
    private final ObjectMapper objectMapper;

    public DriftVectorMessageSerializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setPropertyNamingStrategy(new CamelCaseNamingStrategy());
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder().build();
        this.objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);
    }

    @Override
    public void configure(Map<String, ?> configure, boolean isKey) {
        //No configuration needed
    }

    @Override
    public byte[] serialize(String topic, DriftVectorMessage data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Vector", e);
        }
    }

    @Override
    public void close() {
        // No cleanup needed
    }
}
