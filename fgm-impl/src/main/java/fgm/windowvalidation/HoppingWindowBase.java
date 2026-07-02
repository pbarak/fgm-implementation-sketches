package fgm.windowvalidation;

import fgm.common.serde.records.RecordDeserializer;
import fgm.common.serde.records.RecordSerializer;
import fgm.common.serde.windowaggregate.AggregateResultSerde;
import fgm.common.statevector.HashMapState;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import fgm.streamdata.StreamRecord;
import fgm.windowvalidation.result.AggregateResult;
import fgm.windowvalidation.serde.HashMapStateSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import java.util.HashMap;
import java.util.Map;

public class HoppingWindowBase {

    static Map<String, Object> serdeProps = new HashMap<>();

    static Serde<Long> longSerde = Serdes.Long();
    static Serde<Double> doubleSerde = Serdes.Double();

    static Serde<StreamRecord> streamRecordSerde =
            Serdes.serdeFrom(getRecordSerializer(), getRecordDeserializer());

    static IFgmFactory<?> factory;

    static Deserializer<StreamRecord> getRecordDeserializer() {
        if (factory == null) {
            factory = FactoryInjection.createFactory();
        }
        Deserializer<StreamRecord> recordDeserializer = new RecordDeserializer(factory);
        serdeProps.put("JsonPOJOClass", "fgm.common.serde.records.RecordDeserializer");
        recordDeserializer.configure(serdeProps, false);
        return recordDeserializer;
    }

    static Serializer<StreamRecord> getRecordSerializer() {
        Serializer<StreamRecord> recordSerializer = new RecordSerializer();
        serdeProps.put("JsonPOJOClass", "fgm.common.serde.records.RecordDeserializer");
        recordSerializer.configure(serdeProps, false);
        return recordSerializer;
    }

    static Serde<AggregateResult> aggregateResultSerde = new AggregateResultSerde();

    static Serde<HashMapState> mapSerde = new HashMapStateSerde();


}
