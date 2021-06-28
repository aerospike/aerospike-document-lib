package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
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
     * @param jsonString A given JSON as a String.
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
     * @param jsonNode A given JSON as a JsonNode.
     * @return The given JSON as a Java Map.
     */
    public static Map<String, Object> convertJsonNodeToMap(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(jsonNode, new TypeReference<Map<String, Object>>(){});
    }

    /**
     * Given a serialized JsonNode, return it's equivalent representation as a Java list.
     *
     * @param jsonNode A given JSON as a JsonNode.
     * @return The given JSON as a Java List.
     */
    public static List<Object> convertJsonNodeToList(JsonNode jsonNode) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(jsonNode, new TypeReference<List<Object>>(){});
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
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(map);
    }

    /**
     * Given a list, return its equivalent representation as a Json string.
     *
     * @param list A given Java list.
     * @return The given JSON as a String.
     */
    public static String convertListToJsonString(List<?> list) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(list);
    }
}
