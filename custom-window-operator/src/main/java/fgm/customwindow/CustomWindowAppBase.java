package fgm.customwindow;

import fgm.common.serde.records.RecordDeserializer;
import fgm.common.serde.records.RecordSerializer;
import fgm.common.serde.windowaggregate.AggregateResultSerde;
import fgm.configuration.CfgRetrieval;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;
import fgm.streamdata.StreamRecord;
import fgm.windowvalidation.result.AggregateResult;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.util.HashMap;
import java.util.Map;

public class CustomWindowAppBase {

    static CfgRetrieval cfgRetrieval = new CfgRetrieval();

    static Map<String, Object> serdeProps = new HashMap<>();

    /// ///////////////////////////////////////////////////////////////////////////////////////////
    //Serde declarations

    static Serde<StreamRecord> streamRecordSerde =
            Serdes.serdeFrom(getRecordSerializer(), getRecordDeserializer());

    static Serde<Double> doubleSerde = Serdes.Double();

    static Serde<Windowed<Long>> windowedLongSerde = WindowedSerdes.timeWindowedSerdeFrom(Long.class, cfgRetrieval.getWindowMillisWidth());

    static Serde<AggregateResult> aggregateResultSerde = new AggregateResultSerde();

    /// /////////////////////////////////////////////////////////////////////////////////////////


    static IFgmFactory<?> factory;
    static Deserializer<StreamRecord> getRecordDeserializer() {
        if (factory == null) {
                factory = FactoryInjection.createFactory();
        }
        Deserializer<StreamRecord> recordDeserializer = new RecordDeserializer(factory);
        serdeProps.put("JsonPOJOClass", RecordDeserializer.class.getName());
        recordDeserializer.configure(serdeProps, false);
        return recordDeserializer;
    }

    static Serializer<StreamRecord> getRecordSerializer() {
        Serializer<StreamRecord> recordSerializer = new RecordSerializer();
        serdeProps.put("JsonPOJOClass", RecordSerializer.class.getName());
        recordSerializer.configure(serdeProps, false);
        return recordSerializer;
    }

}
