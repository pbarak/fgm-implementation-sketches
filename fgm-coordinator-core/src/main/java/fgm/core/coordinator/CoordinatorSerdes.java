package fgm.core.coordinator;


import fgm.common.messages.DriftVectorMessage;
import fgm.common.messages.IncrementMessage;
import fgm.common.messages.PhiMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Holds all Serdes used in CoordinatorApplication.
 */
public final class CoordinatorSerdes {

    public final Serde<String> stringSerde;

    public final Serializer<IncrementMessage> incrementSerializer;
    public final Deserializer<IncrementMessage> incrementDeserializer;

    public final Serializer<DriftVectorMessage> driftVectorSerializer;
    public final Deserializer<DriftVectorMessage> driftVectorDeserializer;

    public final Serializer<PhiMessage> phiSerializer;
    public final Deserializer<PhiMessage> phiDeserializer;

    private CoordinatorSerdes() {
        this.stringSerde = CoordinatorSerdesFactory.stringSerde();

        this.incrementSerializer = CoordinatorSerdesFactory.incrementSerializer();
        this.incrementDeserializer = CoordinatorSerdesFactory.incrementDeserializer();

        this.driftVectorSerializer = CoordinatorSerdesFactory.driftVectorSerializer();
        this.driftVectorDeserializer = CoordinatorSerdesFactory.driftVectorDeserializer();

        this.phiSerializer = CoordinatorSerdesFactory.phiSerializer();
        this.phiDeserializer = CoordinatorSerdesFactory.phiDeserializer();
    }

    public static CoordinatorSerdes create() {
        return new CoordinatorSerdes();
    }
}

