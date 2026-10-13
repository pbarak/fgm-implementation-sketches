package fgm.common.serde.records;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import fgm.streamdata.StreamRecord;
import fgm.streamdata.implementation.stream.WorldCupRecord;

import java.io.IOException;


public class StreamRecordInterfaceDeserializer
        extends JsonDeserializer<StreamRecord> {

    private static final String WORLD_CUP = "WorldCupRecord";

    @Override
    public StreamRecord deserialize(
            JsonParser p,
            DeserializationContext ctxt
    ) throws IOException {
        JsonNode node = p.getCodec().readTree(p);

        // guard missing or wrong type
        if (!node.hasNonNull("record_type")
                || !WORLD_CUP.equals(node.get("record_type").asText())) {
            return null;
        }

        WorldCupRecord wc = new WorldCupRecord();
        // use asLong() so you never hit parseLong("null")
        wc.setTimeStamp(node.path("time_stamp").asLong());
        wc.setClientID(node.path("client_i_d").asLong());
        wc.setObjectID(node.path("object_i_d").asLong());
        wc.setSize(node.path("size").asLong());
        wc.setMethod(node.path("method").shortValue());
        wc.setStatus(node.path("status").shortValue());
        wc.setType(node.path("type").shortValue());
        wc.setServer(node.path("server").shortValue());
        return wc;
    }
}
