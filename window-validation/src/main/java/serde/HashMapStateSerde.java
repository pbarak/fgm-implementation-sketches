package fgm.windowvalidation.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import fgm.common.statevector.HashMapState;
import org.apache.kafka.common.serialization.*;

import java.util.Map;

public class HashMapStateSerde implements Serde<HashMapState> {
    private final Serializer<HashMapState> serializer;
    private final Deserializer<HashMapState> deserializer;

    public HashMapStateSerde() {
        // Create a shared mapper (you can configure it as needed)
        ObjectMapper mapper = new ObjectMapper();
        this.serializer = new HashMapStateSerializer(mapper);
        this.deserializer = new HashMapStateDeserializer(mapper);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public Serializer<HashMapState> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<HashMapState> deserializer() {
        return deserializer;
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }
}