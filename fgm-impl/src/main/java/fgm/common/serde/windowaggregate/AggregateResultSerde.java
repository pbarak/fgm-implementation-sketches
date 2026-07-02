package fgm.common.serde.windowaggregate;


import fgm.common.serde.JsonPOJODeserializer;
import fgm.common.serde.JsonPOJOSerializer;
import fgm.windowvalidation.result.AggregateResult;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

public class AggregateResultSerde implements Serde<AggregateResult> {

    private static final String SERDES_PROPERTY = "JsonPOJOClass";
    private final Serde<AggregateResult> inner;

    public AggregateResultSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put(SERDES_PROPERTY, AggregateResult.class);

        final Serializer<AggregateResult> serializer = new JsonPOJOSerializer<>();
        serializer.configure(serdeProps, false);

        final Deserializer<AggregateResult> deserializer = new JsonPOJODeserializer<>();
        deserializer.configure(serdeProps, false);

        this.inner = Serdes.serdeFrom(serializer, deserializer);
    }

    @Override
    public Serializer<AggregateResult> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<AggregateResult> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.close();
    }
}

