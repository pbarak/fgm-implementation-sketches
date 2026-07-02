package fgm.windowvalidation.serde;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fgm.common.statevector.HashMapState;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;

import java.util.HashMap;
import java.util.Map;

public class HashMapStateDeserializer implements Deserializer<HashMapState> {
    private final ObjectMapper objectMapper;

    public HashMapStateDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { /* no-op */ }

    @Override
    public HashMapState deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0)
            return new HashMapState();
        try {
            // Read JSON as Map<String,Double> then convert keys to Long
            Map<Long, Double> map = objectMapper.readValue(
                    bytes,
                    new TypeReference<Map<Long, Double>>() {
                    }
            );
            // Wrap in your state class (makes a shallow copy)
            return new HashMapState(new HashMap<>(map));
        } catch (Exception e) {
            throw new SerializationException("Error deserializing HashMapState from JSON", e);
        }
    }

    @Override
    public void close() { /* no-op */ }
}
