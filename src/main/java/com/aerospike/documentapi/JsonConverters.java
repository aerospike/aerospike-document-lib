package com.aerospike.documentapi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonConverters {

    /**
     * Given a serialized json object, return it's equivalent representation as a JsonNode.
     *
     * @param jsonString a given JSON as a String.
     * @return The given JSON as a JsonNode.
     * @throws IOException an IOException will be thrown in case of an error.
     */
    public static JsonNode convertStringToJsonNode(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString, JsonNode.class);
    }

    /**
     * Given a serialized JsonNode, return it's equivalent representation as a Java map.
     *
     * @param jsonNode a given JSON as a JsonNode.
     * @return The given JSON as a Java Map.
     */
    public static Map<String, Object> convertJsonNodeToMap(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>(){});
    }

    /**
     * Given a serialized JsonNode, return it's equivalent representation as a Java list.
     *
     * @param jsonNode a given JSON as a JsonNode.
     * @return The given JSON as a Java List.
     */
    public static List<Object> convertJsonNodeToList(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(jsonNode, new TypeReference<List<Object>>(){});
    }
}
