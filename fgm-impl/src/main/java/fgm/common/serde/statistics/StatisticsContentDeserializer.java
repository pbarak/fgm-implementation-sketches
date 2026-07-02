package fgm.common.serde.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import fgm.common.messages.StatisticsMessage;
import fgm.streamdata.implementation.stream.WorldCupRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsContentDeserializer extends JsonDeserializer<StatisticsMessage> {
    private static final Logger logger = LoggerFactory.getLogger(StatisticsContentDeserializer.class);
    private static final String WORLD_CUP_RECORD = "WorldCupRecord";
    private static final String RECORD_CONSTANT = "record";

    @Override
    public StatisticsMessage deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder().build();
            mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);
            JsonNode node = mapper.readTree(jsonParser);
            StatisticsMessage statisticsMessage = StatisticsMessage.getInstance();

            if (node.get("message_type") == null) {
                System.out.println("test");
            }
            statisticsMessage.setMessageType(node.get("message_type").asText());
            statisticsMessage.setRound(Integer.valueOf(node.get("round").asText()));
            statisticsMessage.setSubround(Integer.valueOf(node.get("subround").asText()));
            statisticsMessage.setTimeStamp(Long.parseLong(node.get("time_stamp").asText()));
            statisticsMessage.setSiteID(node.get("site_i_d").asText());

            if ((!node.get(RECORD_CONSTANT).equals(NullNode.getInstance())) && (node.get(RECORD_CONSTANT) != null)) {
                if (node.get(RECORD_CONSTANT).get("record_type").asText().equals(WORLD_CUP_RECORD)) {
                    WorldCupRecord wc = new WorldCupRecord();
                    wc.setClientID(Long.parseLong(node.get(RECORD_CONSTANT).get("client_i_d").asText()));
                    wc.setServer(Long.parseLong(node.get(RECORD_CONSTANT).get("server").asText()));
                    wc.setTimeStamp(Long.parseLong(node.get(RECORD_CONSTANT).get("time_stamp").asText()));
                    statisticsMessage.setRecord(wc);
                    return statisticsMessage;
                }
            }

            return statisticsMessage;
        } catch (Exception e) {
            logger.info("Error while deserializing statistics message.", e);
        }
        return null;
    }
}
