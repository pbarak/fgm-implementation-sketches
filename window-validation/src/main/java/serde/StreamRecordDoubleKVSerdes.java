package fgm.windowvalidation.serde;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fgm.common.serde.records.RecordDeserializer;
import fgm.common.serde.records.RecordSerializer;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import fgm.streamdata.StreamRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;

import java.util.HashMap;
import java.util.Map;

/**
 * JSON Serde for KeyValue<StreamRecord, Double>.
 */
public class StreamRecordDoubleKVSerdes implements Serde<KeyValue<StreamRecord, Double>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE =
            new TypeReference<Map<String, Object>>() {/* anonymous subclass */
            };

    /* ---------- Serializer ---------- */
    private static final class KVSerializer implements Serializer<KeyValue<StreamRecord, Double>> {
        private final RecordSerializer recordSerializer = new RecordSerializer();

        @Override
        public byte[] serialize(String topic, KeyValue<StreamRecord, Double> data) {
            if (data == null) {
                return new byte[0];
            }
            try {
                Map<String, Object> map = new HashMap<>();
                map.put("key", OBJECT_MAPPER.readTree(recordSerializer.serialize(topic, data.key)));
                map.put("value", data.value);
                return OBJECT_MAPPER.writeValueAsBytes(map);
            } catch (Exception e) {
                throw new RuntimeException("Could not JSON-serialise KeyValue<StreamRecord,Double>", e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public void close() {
        }
    }

    /* ---------- Deserializer ---------- */
    private static final class KVDeserializer implements Deserializer<KeyValue<StreamRecord, Double>> {
        private static final IFgmFactory<?> FACTORY;
        static {
            FACTORY = FactoryInjection.createFactory();
        }

        private final RecordDeserializer recordDeserializer = new RecordDeserializer(FACTORY);

        @Override
        public KeyValue<StreamRecord, Double> deserialize(String topic, byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            try {
                Map<String, Object> map = OBJECT_MAPPER.readValue(bytes, MAP_TYPE);
                byte[] keyBytes = OBJECT_MAPPER.writeValueAsBytes(map.get("key"));
                StreamRecord key = recordDeserializer.deserialize(topic, keyBytes);
                Double value = ((Number) map.get("value")).doubleValue();
                return KeyValue.pair(key, value);
            } catch (Exception e) {
                throw new RuntimeException("Could not JSON-deserialise KeyValue<StreamRecord,Double>", e);
            }
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public void close() {
        }
    }

    private final Serializer<KeyValue<StreamRecord, Double>> ser = new KVSerializer();
    private final Deserializer<KeyValue<StreamRecord, Double>> de = new KVDeserializer();

    @Override
    public Serializer<KeyValue<StreamRecord, Double>> serializer() {
        return ser;
    }

    @Override
    public Deserializer<KeyValue<StreamRecord, Double>> deserializer() {
        return de;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }
}

