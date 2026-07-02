package fgm.core.site;

import fgm.common.cfg.BaseConfiguration;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.StatisticsMessage;
import fgm.common.serde.JsonPOJOSerializer;
import fgm.common.serde.records.RecordDeserializer;
import fgm.common.serde.statistics.StatisticsMessageSerializer;
import fgm.factory.specs.IFgmFactory;
import org.apache.kafka.common.serialization.*;

import java.util.HashMap;
import java.util.Map;

import static fgm.core.site.SiteApplicationBase.JSON_SERDES_CLASS_NAME;

public final class SiteSerdes {

    final Serde<String> stringSerde;
    final Deserializer<String> stringDeserializer;

    final Serde<Double> doubleSerde;
    final Deserializer<Double> doubleDeserializer;

    final Serializer<IncrementMessage> incrementMessageSerializer;
    final Serializer<StatisticsMessage> statisticsMessageSerializer;

    final Deserializer<?> recordDeserializer;

    private SiteSerdes(IFgmFactory<?> factory, BaseConfiguration baseConfiguration) {
        this.stringSerde = Serdes.String();
        this.stringDeserializer = stringSerde.deserializer();

        this.doubleSerde = Serdes.Double();
        this.doubleDeserializer = doubleSerde.deserializer();

        Map<String, Object> props = new HashMap<>();

        this.incrementMessageSerializer = new JsonPOJOSerializer<>();
        props.put(JSON_SERDES_CLASS_NAME, IncrementMessage.class);
        incrementMessageSerializer.configure(props, false);

        this.statisticsMessageSerializer = new StatisticsMessageSerializer();
        props.put(JSON_SERDES_CLASS_NAME, StatisticsMessage.class);
        statisticsMessageSerializer.configure(props, false);

        this.recordDeserializer = new RecordDeserializer(factory);
        props.put(JSON_SERDES_CLASS_NAME, RecordDeserializer.class.getName());
        recordDeserializer.configure(props, false);
    }

    public static SiteSerdes create(IFgmFactory<?> factory, BaseConfiguration baseConfiguration) {
        return new SiteSerdes(factory, baseConfiguration);
    }
}

