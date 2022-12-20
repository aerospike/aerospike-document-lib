package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.DocumentApiException;
import com.aerospike.documentapi.util.JsonConverters;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONArray;

public class JsonPathQuery {

    static final String DOCUMENT_ROOT = "$";

    private JsonPathQuery() {
    }

    /**
     * Retrieve the objects that match JSON path query.
     *
     * @param jsonPathObject parsed JSON path.
     * @param json           an object that represents a list or a map (e.g., Aerospike database result).
     * @return list of objects matched by the given path.
     * @throws DocumentApiException if there was validation error.
     */
    public static Object read(JsonPathObject jsonPathObject, Object json) {
        validateNotNull(json);

        try {
            String resultJson = JsonConverters.writeValueAsString(json);
            String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
            return JsonPath.read(resultJson, jsonPath);
        } catch (Exception e) {
            throw DocumentApiException.toDocumentException(e);
        }
    }

    /**
     * Put a value according to JSON path query and return the updated JSON.
     *
     * @param jsonPathObject parsed JSON path.
     * @param json           an object that represents a list or a map (e.g., Aerospike database result).
     * @param value          an object to put.
     * @return updated JSON.
     * @throws DocumentApiException if there was an error.
     */
    public static Object putOrSet(JsonPathObject jsonPathObject, Object json, Object value) {
        validateNotNull(json);

        try {
            String resultJson = JsonConverters.writeValueAsString(json);
            String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
            JSONArray keys = JsonPath.parse(resultJson).read(jsonPath);
            // if jsonPath exists or if it leads to an array element
            if (!keys.isEmpty() || jsonPath.charAt(jsonPath.length() - 1) == ']') {
                return setOrAdd(resultJson, jsonPath, value);
            }
            // if jsonPath does not exist in json, and it leads to a map element
            return put(resultJson, jsonPath, value);
        } catch (Exception e) {
            throw DocumentApiException.toDocumentException(e);
        }
    }

    private static Object put(String resultJson, String jsonPath, Object value) {
        String key = jsonPath.substring(jsonPath.lastIndexOf(".") + 1);
        jsonPath = jsonPath.substring(0, jsonPath.lastIndexOf("."));
        return JsonPath.parse(resultJson).put(jsonPath, key, value).json();
    }

    private static Object setOrAdd(String resultJson, String jsonPath, Object value) {
        try {
            return JsonPath.parse(resultJson).set(jsonPath, value).json();
        } catch (PathNotFoundException e) {
            // adding the path because it does not exist
            // add() requires path to an array, so the path to a particular element is omitted
            String arrPath = jsonPath.substring(0, jsonPath.lastIndexOf('['));
            return JsonPath.parse(resultJson).add(arrPath, value).json();
        }
    }

    /**
     * Append a value according to JSON path query and return the updated JSON.
     *
     * @param jsonPathObject parsed JSON path.
     * @param json           an object that represents a list or a map (e.g., Aerospike database result).
     * @param value          an object to append.
     * @return updated JSON.
     * @throws DocumentApiException if there was an error.
     */
    public static Object append(JsonPathObject jsonPathObject, Object json, Object value) {
        validateNotNull(json);

        try {
            String resultJson = JsonConverters.writeValueAsString(json);
            String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
            return JsonPath.parse(resultJson).add(jsonPath, value).json();
        } catch (Exception e) {
            throw DocumentApiException.toDocumentException(e);
        }
    }

    /**
     * Delete an element according to JSON path query and return the updated JSON.
     *
     * @param jsonPathObject parsed JSON path.
     * @param json           an object that represents a list or a map (e.g., Aerospike database result).
     * @return updated JSON.
     * @throws DocumentApiException if there was an error.
     */
    public static Object delete(JsonPathObject jsonPathObject, Object json) {
        validateNotNull(json);

        try {
            String resultJson = JsonConverters.writeValueAsString(json);
            String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
            return JsonPath.parse(resultJson).delete(jsonPath).json();
        } catch (Exception e) {
            throw DocumentApiException.toDocumentException(e);
        }
    }

    private static void validateNotNull(Object json) {
        if (json == null) throw new DocumentApiException("Json object for performing path query is null");
    }
}
