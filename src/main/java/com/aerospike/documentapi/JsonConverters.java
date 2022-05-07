package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class JsonConverters {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Given a serialized json object, return its equivalent representation as a JsonNode.
     *
     * @param jsonString A given JSON as a String.
     * @return The given JSON as a JsonNode.
     * @throws IOException an IOException will be thrown in case of an error.
     */
    public static JsonNode convertStringToJsonNode(String jsonString) throws IOException {
        return mapper.readValue(jsonString, JsonNode.class);
    }

    /**
     * Given a serialized JsonNode, return its equivalent representation as a Java map.
     *
     * @param jsonNode A given JSON as a JsonNode.
     * @return The given JSON as a Java Map.
     */
    public static Map<String, Object> convertJsonNodeToMap(JsonNode jsonNode) {
        return mapper.convertValue(jsonNode, new TypeReference<TreeMap<String, Object>>() {
        });
    }

    /**
     * Given a serialized JsonNode, return its equivalent representation as a Java list.
     *
     * @param jsonNode A given JSON as a JsonNode.
     * @return The given JSON as a Java List.
     */
    public static List<Object> convertJsonNodeToList(JsonNode jsonNode) {
        return mapper.convertValue(jsonNode, new TypeReference<List<Object>>() {
        });
    }

    /**
     * Given an object that represents a list or a map for example an Aerospike database result,
     * return its equivalent representation as a Json string.
     *
     * @param object A given object (list or map).
     * @return The given JSON as a String.
     */
    public static String convertObjectToJsonString(Object object) throws JsonProcessingException {
        String resultJson;
        if (object instanceof List<?>) {
            resultJson = JsonConverters.convertListToJsonString((List<?>) object);
        } else {
            resultJson = JsonConverters.convertMapToJsonString((Map<?, ?>) object);
        }
        return resultJson;
    }

    /**
     * Given a map, return its equivalent representation as a Json string.
     *
     * @param map A given Java map.
     * @return The given JSON as a String.
     */
    public static String convertMapToJsonString(Map<?, ?> map) throws JsonProcessingException {
        return mapper.writeValueAsString(map);
    }

    /**
     * Given a list, return its equivalent representation as a Json string.
     *
     * @param list A given Java list.
     * @return The given JSON as a String.
     */
    public static String convertListToJsonString(List<?> list) throws JsonProcessingException {
        return mapper.writeValueAsString(list);
    }
}
