package fgm.windowvalidation.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import fgm.common.statevector.HashMapState;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;

import java.util.Map;

public class HashMapStateSerializer implements Serializer<HashMapState> {
    private final ObjectMapper objectMapper;

    public HashMapStateSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { /* no-op */ }

    @Override
    public byte[] serialize(String topic, HashMapState data) {
        if (data == null) return new byte[0];
        try {
            // JSON-encode the internal Map<Long,Double>
            return objectMapper.writeValueAsBytes(data.getState());
        } catch (Exception e) {
            throw new SerializationException("Error serializing HashMapState to JSON", e);
        }
    }

    @Override
    public void close() { /* no-op */ }
}
