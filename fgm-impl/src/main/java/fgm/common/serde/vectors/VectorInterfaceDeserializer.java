package fgm.common.serde.vectors;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import fgm.common.messages.DriftVectorMessage;
import fgm.common.statevector.FastAGMS;

import java.io.IOException;
import java.util.*;

import fgm.common.statevector.HashMapState;
import fgm.common.statevector.KeyFrequencyList;
import fgm.factory.FactoryInjection;
import fgm.factory.specs.IFgmFactory;

public class VectorInterfaceDeserializer extends JsonDeserializer<DriftVectorMessage> {

    private static final String STATE_VECTOR = "state_vector";
    private static final String TYPE = "type";
    private static final String FAST_AGMS = "FastAGMS";
    private static final String KEY_FREQUENCY_LIST_KEY = "KeyFrequencyList";
    private static final String HASH_MAP_STATE = "HashMapState";
    private static final String CURRENT_ROUND = "current_round";
    private static final String CURRENT_SUBROUND = "current_subround";
    private static final String COUNTS = "counts";
    private static final String SITE_ID = "site_id";

    // KeyFrequencyList-specific field names
    private static final String STATE_VECTOR_LIST_CAMEL = "stateVector";
    private static final String STATE_VECTOR_LIST_SNAKE = "state_vector";
    private static final String ENTRY_KEY = "key";
    private static final String ENTRY_VALUE = "value";
    private static final Double ZERO_VALUE = 0.0;

    //HashMapState-specific field names
    private static final String STATE_MAP = "state";

    @Override
    public DriftVectorMessage deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder().build();
        mapper.activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE);
        JsonNode node = mapper.readTree(jsonParser);
        if (node.get(STATE_VECTOR).has(TYPE)) {
            String type = node.get(STATE_VECTOR).get(TYPE).asText();
            if (FAST_AGMS.equals(type)) {
                DriftVectorMessage driftVectorMessage = new DriftVectorMessage();
                driftVectorMessage.setCurrentRound(mapper.readValue(node.get(CURRENT_ROUND).toString(), Integer.class));
                driftVectorMessage.setSiteId(mapper.readValue(node.get(SITE_ID).toString(), String.class));
                driftVectorMessage.setCurrentSubround(mapper.readValue(node.get(CURRENT_SUBROUND).toString(), Integer.class));
                IFgmFactory<?> factory = FactoryInjection.createFactory();
                FastAGMS fastAGMS = FastAGMS.newInstance(factory.getBaseCfg());
                fastAGMS.setCounts(mapper.readValue(node.get(STATE_VECTOR).get(COUNTS).toString(), double[][].class));
                driftVectorMessage.setStateVector(fastAGMS);
                return driftVectorMessage;
            } else if (KEY_FREQUENCY_LIST_KEY.equals(type)) {
                DriftVectorMessage driftVectorMessage = new DriftVectorMessage();
                driftVectorMessage.setCurrentRound(mapper.readValue(node.get(CURRENT_ROUND).toString(), Integer.class));
                driftVectorMessage.setSiteId(mapper.readValue(node.get(SITE_ID).toString(), String.class));
                driftVectorMessage.setCurrentSubround(mapper.readValue(node.get(CURRENT_SUBROUND).toString(), Integer.class));

                JsonNode svNode = node.get(STATE_VECTOR);
                // prefer camelCase; fall back to snake_case
                JsonNode listNode = svNode.get(STATE_VECTOR_LIST_CAMEL);
                if (listNode == null) listNode = svNode.get(STATE_VECTOR_LIST_SNAKE);

                List<Map.Entry<Long, Double>> entries = parseKeyFrequencyListNode(listNode);
                KeyFrequencyList kfl = new KeyFrequencyList(entries);
                // safe about ordering:
                kfl.sortStateVector();

                driftVectorMessage.setStateVector(kfl);
                return driftVectorMessage;
            } else if (HASH_MAP_STATE.equals(type)) {
                DriftVectorMessage dvm = new DriftVectorMessage();
                dvm.setCurrentRound(mapper.readValue(node.get(CURRENT_ROUND).toString(), Integer.class));
                dvm.setSiteId(mapper.readValue(node.get(SITE_ID).toString(), String.class));
                dvm.setCurrentSubround(mapper.readValue(node.get(CURRENT_SUBROUND).toString(), Integer.class));

                // Typical shape: state_vector: { type: "HashMapState", state: { "123":"1.5", "456":2 } }
                // Be resilient: if "state" is missing, try to parse the container itself.
                JsonNode stateNode = node.get(STATE_VECTOR).get(STATE_MAP);
                Map<Long, Double> map = parseHashMapStateNode(stateNode != null ? stateNode : node.get(STATE_VECTOR));

                HashMapState hms = new HashMapState(map);
                dvm.setStateVector(hms);
                return dvm;
            }
        }
        return null;
    }


    /**
     * Parses KeyFrequencyList from a node that may be default-typed or plain.
     */
    private static List<Map.Entry<Long, Double>> parseKeyFrequencyListNode(JsonNode listNode) {
        List<Map.Entry<Long, Double>> out = new ArrayList<>();
        if (listNode == null || listNode.isNull()) return out;

        // Case A: default-typed wrapper: [ "java.util.ArrayList", [ ...items... ] ]
        if (listNode.isArray() && listNode.size() == 2 && listNode.get(0).isTextual() && listNode.get(1).isArray()) {
            ArrayNode inner = (ArrayNode) listNode.get(1);
            for (JsonNode item : inner) {
                parseOneEntryPossiblyWrapped(item, out);
            }
            return out;
        }

        // Case B: plain array
        if (listNode.isArray()) {
            for (JsonNode item : listNode) {
                parseOneEntryPossiblyWrapped(item, out);
            }
            return out;
        }

        // Case C: a single object
        parseOneEntryPossiblyWrapped(listNode, out);
        return out;
    }

    /**
     * Parses a single entry which might be wrapped like ["java.util.AbstractMap$SimpleEntry", { "k": v }]
     */
    private static void parseOneEntryPossiblyWrapped(JsonNode node, List<Map.Entry<Long, Double>> out) {
        // Unwrap default-typed entry array
        if (node.isArray() && node.size() == 2 && node.get(0).isTextual() && node.get(1).isObject()) {
            node = node.get(1);
        }

        // Case 1: Jackson Map.Entry default serializer -> object with a single field: { "<key>": <value> }
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            Iterator<String> it = obj.fieldNames();
            while (it.hasNext()) {
                String k = it.next();
                JsonNode vNode = obj.get(k);
                long key = parseLongFlexible(k);
                double val = vNode != null && vNode.isNumber() ? vNode.asDouble()
                        : vNode != null ? Double.parseDouble(vNode.asText())
                        : ZERO_VALUE;
                out.add(new AbstractMap.SimpleEntry<>(key, val));
            }
            return;
        }

        // Case 2: Explicit { "key": ..., "value": ... } shape (if you ever switch)
        if (node.has(ENTRY_KEY) && node.has(ENTRY_VALUE)) {
            long key = node.get(ENTRY_KEY).isNumber() ? node.get(ENTRY_KEY).asLong()
                    : Long.parseLong(node.get(ENTRY_KEY).asText());
            double val = node.get(ENTRY_VALUE).isNumber() ? node.get(ENTRY_VALUE).asDouble()
                    : Double.parseDouble(node.get(ENTRY_VALUE).asText());
            out.add(new AbstractMap.SimpleEntry<>(key, val));
        }
    }

    private static long parseLongFlexible(String s) {
        // Accept both numeric-looking strings and plain numbers-as-strings
        return Long.parseLong(s.trim());
    }

    private static Map<Long, Double> parseHashMapStateNode(JsonNode node) {
        Map<Long, Double> out = new HashMap<>();
        if (node == null || node.isNull()) return out;

        // Default-typed wrapper: [ "java.util.HashMap", { ... } ]
        if (node.isArray() && node.size() == 2 && node.get(0).isTextual()) {
            JsonNode inner = node.get(1);
            if (inner != null) return parseHashMapStateNode(inner);
        }

        // Plain object: { "123": 1.0, "456": "2.5", ... }
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            Iterator<String> it = obj.fieldNames();
            while (it.hasNext()) {
                String k = it.next();
                long key = parseLongFlexible(k);
                JsonNode vNode = obj.get(k);
                double val = vNode != null && vNode.isNumber() ? vNode.asDouble()
                        : vNode != null ? Double.parseDouble(vNode.asText())
                        : ZERO_VALUE;
                out.put(key, val);
            }
            return out;
        }

        // Array of entries: [{key:..., value:...}] or [{ "123": 4.0 }]
        if (node.isArray()) {
            for (JsonNode item : node) {
                parseMapEntryNode(item, out);
            }
            return out;
        }

        // Single entry fallback
        parseMapEntryNode(node, out);
        return out;
    }

    private static void parseMapEntryNode(JsonNode item, Map<Long, Double> out) {
        // Unwrap default-typed entry: ["java.util.AbstractMap$SimpleEntry", { ... }]
        if (item.isArray() && item.size() == 2 && item.get(0).isTextual() && item.get(1).isObject()) {
            item = item.get(1);
        }

        if (item.isObject()) {
            // { "key":..., "value":... }
            if (item.has(ENTRY_KEY) && item.has(ENTRY_VALUE)) {
                long k = item.get(ENTRY_KEY).isNumber() ? item.get(ENTRY_KEY).asLong()
                        : Long.parseLong(item.get(ENTRY_KEY).asText());
                double v = item.get(ENTRY_VALUE).isNumber() ? item.get(ENTRY_VALUE).asDouble()
                        : Double.parseDouble(item.get(ENTRY_VALUE).asText());
                out.put(k, v);
                return;
            }
            // { "<onlyFieldKey>": <value> }
            Iterator<String> it = ((ObjectNode) item).fieldNames();
            while (it.hasNext()) {
                String k = it.next();
                JsonNode vNode = item.get(k);
                long key = parseLongFlexible(k);
                double val = vNode != null && vNode.isNumber() ? vNode.asDouble()
                        : vNode != null ? Double.parseDouble(vNode.asText())
                        : ZERO_VALUE;
                out.put(key, val);
            }
            return;
        }

        // ["123", 4.0] or [123, 4.0]
        if (item.isArray() && item.size() == 2) {
            JsonNode kNode = item.get(0);
            JsonNode vNode = item.get(1);
            long key = kNode.isNumber() ? kNode.asLong() : Long.parseLong(kNode.asText());
            double val = vNode.isNumber() ? vNode.asDouble() : Double.parseDouble(vNode.asText());
            out.put(key, val);
        }
    }

}
