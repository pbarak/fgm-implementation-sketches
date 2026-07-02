package fgm.common.serde.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import fgm.common.messages.StatisticsMessage;
import fgm.common.serde.common.CamelCaseNamingStrategy;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class StatisticsMessageSerializer implements Serializer<StatisticsMessage> {

    private final ObjectMapper objectMapper;

    public StatisticsMessageSerializer() {
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
    public byte[] serialize(String topic, StatisticsMessage data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Statistics Message", e);
        }
    }

    @Override
    public void close() {
        // No cleanup needed
    }
}
