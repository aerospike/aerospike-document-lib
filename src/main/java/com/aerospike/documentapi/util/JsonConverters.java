package com.aerospike.documentapi.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@UtilityClass
public class JsonConverters {

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Given a serialized Json object, return its equivalent representation as a JsonNode.
     *
     * @param jsonString the Json string representation.
     * @return the given string as a JsonNode.
     */
    public static JsonNode convertStringToJsonNode(String jsonString) {
        try {
            return mapper.readValue(jsonString, JsonNode.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Given a serialized JsonNode, return its equivalent representation as a Java map.
     *
     * @param jsonNode the JsonNode to convert.
     * @return the given JsonNode as a Java Map.
     */
    public static Map<String, Object> convertJsonNodeToMap(JsonNode jsonNode) {
        return mapper.convertValue(jsonNode, new TypeReference<TreeMap<String, Object>>() {
        });
    }

    /**
     * Given a serialized JsonNode, return its equivalent representation as a Java list.
     *
     * @param jsonNode the JsonNode to convert.
     * @return the given JsonNode as a Java List.
     */
    public static List<Object> convertJsonNodeToList(JsonNode jsonNode) {
        return mapper.convertValue(jsonNode, new TypeReference<List<Object>>() {
        });
    }

    /**
     * Given an object that represents a list or a map for example an Aerospike database result,
     * return its equivalent representation as a Json string.
     *
     * @param object the object to parse as a Json string.
     * @return the JSON string representation of the object.
     */
    public static String writeValueAsString(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
