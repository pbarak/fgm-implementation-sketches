package fgm.core.coordinator;

import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import fgm.common.serde.JsonPOJODeserializer;
import fgm.common.serde.JsonPOJOSerializer;
import fgm.common.serde.vectors.DriftVectorMessageDeserializer;
import fgm.common.serde.vectors.DriftVectorMessageSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

import static fgm.core.coordinator.CoordinatorApplicationBase.JSON_SERDES_CLASS_NAME;

/**
 * Factory for all SerDes used in CoordinatorApplication.
 */
public final class CoordinatorSerdesFactory {

    private CoordinatorSerdesFactory() {
    }

    public static Serde<String> stringSerde() {
        return Serdes.String();
    }

    public static Serializer<IncrementMessage> incrementSerializer() {
        Serializer<IncrementMessage> serializer = new JsonPOJOSerializer<>();
        serializer.configure(configFor(IncrementMessage.class), false);
        return serializer;
    }

    public static Deserializer<IncrementMessage> incrementDeserializer() {
        Deserializer<IncrementMessage> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(configFor(IncrementMessage.class), false);
        return deserializer;
    }

    public static Serializer<DriftVectorMessage> driftVectorSerializer() {
        Serializer<DriftVectorMessage> serializer = new DriftVectorMessageSerializer();
        serializer.configure(configFor(DriftVectorMessage.class), false);
        return serializer;
    }

    public static Deserializer<DriftVectorMessage> driftVectorDeserializer() {
        Deserializer<DriftVectorMessage> deserializer = new DriftVectorMessageDeserializer();
        deserializer.configure(configFor(DriftVectorMessage.class), false);
        return deserializer;
    }

    public static Serializer<PhiMessage> phiSerializer() {
        Serializer<PhiMessage> serializer = new JsonPOJOSerializer<>();
        serializer.configure(configFor(PhiMessage.class), false);
        return serializer;
    }

    public static Deserializer<PhiMessage> phiDeserializer() {
        Deserializer<PhiMessage> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(configFor(PhiMessage.class), false);
        return deserializer;
    }

    private static Map<String, Object> configFor(Class<?> clazz) {
        Map<String, Object> props = new HashMap<>();
        props.put(JSON_SERDES_CLASS_NAME, clazz);
        return props;
    }
}

