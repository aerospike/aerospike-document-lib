package com.aerospike.documentapi;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class Utils {

    /**
     * Given a serialized json object, return it's equivalent representation as a Java map
     *
     * @param jsonString a given JSON as a String.
     * @return The given JSON as a Java Map.
     * @throws IOException an IOException will be thrown in case of an error.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> convertJSONFromStringToMap(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // Reading string into a map
        return (Map<String, Object>) mapper.readValue(jsonString, Map.class);
    }
}
