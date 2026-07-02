package fgm.common.serde.records;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import fgm.common.serde.common.CamelCaseNamingStrategy;
import fgm.factory.specs.IFgmFactory;
import fgm.streamdata.StreamRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class RecordDeserializer implements Deserializer<StreamRecord> {
    private final ObjectMapper objectMapper;
    private final JsonDeserializer<StreamRecord> deserializer;

    public RecordDeserializer(IFgmFactory<?> factory) {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setPropertyNamingStrategy(new CamelCaseNamingStrategy());

        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder().build();
        objectMapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);

        deserializer = factory.getRecordDeserializer();

        SimpleModule module = new SimpleModule();
        module.addDeserializer(StreamRecord.class, deserializer);
        objectMapper.registerModule(module);


    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //No configuration needed
    }

    @Override
    public StreamRecord deserialize(String topic, byte[] data) {
        try {
            DeserializationContext ctxt = objectMapper.getDeserializationContext();
            JsonParser jsonParser = objectMapper.getFactory().createParser(data);
            return deserializer.deserialize(jsonParser, ctxt);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Vector", e);
        }
    }

    @Override
    public void close() {
        //No configuration needed
    }


}
