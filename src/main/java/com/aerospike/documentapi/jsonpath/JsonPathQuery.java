package com.aerospike.documentapi.jsonpath;

import com.aerospike.client.AerospikeException;
import com.aerospike.documentapi.DocumentApiException;
import com.aerospike.documentapi.util.JsonConverters;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import lombok.SneakyThrows;
import net.minidev.json.JSONArray;

public class JsonPathQuery {

    static final String DOCUMENT_ROOT = "$";

    private JsonPathQuery() {
    }

    /**
     * @param jsonPathObject
     * @param object
     * @return result of applying the operation to the parsed JSON.
     * @throws DocumentApiException.ObjectNotFoundException if object was passed as null.
     * @throws JsonPathParser.JsonParseException            if there was an exception during JSON parsing.
     */
    @SneakyThrows
    public static Object read(JsonPathObject jsonPathObject, Object object) {
        validate(object);

        String resultJson = JsonConverters.writeValueAsString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.read(resultJson, jsonPath);
    }

    /**
     * @param jsonPathObject
     * @param object
     * @param value
     * @return result of applying the operation to the parsed JSON.
     * @throws DocumentApiException.ObjectNotFoundException if object was passed as null.
     * @throws JsonPathParser.JsonParseException            if there was an exception during JSON parsing.
     */
    @SneakyThrows
    public static Object putOrSet(JsonPathObject jsonPathObject, Object object, Object value) {
        validate(object);

        String resultJson = JsonConverters.writeValueAsString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        JSONArray keys = JsonPath.parse(resultJson).read(jsonPath);
        // if jsonPath exists or if it leads to an array element
        if (!keys.isEmpty() || jsonPath.charAt(jsonPath.length() - 1) == ']') {
            return setOrAdd(resultJson, jsonPath, value);
        }
        // if jsonPath does not exist in json, and it leads to a map element
        return put(resultJson, jsonPath, value);
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
     * @param jsonPathObject
     * @param object
     * @param value
     * @return result of applying the operation to the parsed JSON.
     * @throws DocumentApiException.ObjectNotFoundException if object was passed as null.
     * @throws JsonPathParser.JsonParseException            if there was an exception during JSON parsing.
     */
    @SneakyThrows
    public static Object append(JsonPathObject jsonPathObject, Object object, Object value) {
        validate(object);

        String resultJson = JsonConverters.writeValueAsString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).add(jsonPath, value).json();
    }

    /**
     * @param jsonPathObject
     * @param object
     * @return result of applying the operation to the parsed JSON.
     * @throws DocumentApiException.ObjectNotFoundException if object was passed as null.
     * @throws JsonPathParser.JsonParseException            if there was an exception during JSON parsing.
     */
    @SneakyThrows
    public static Object delete(JsonPathObject jsonPathObject, Object object) {
        validate(object);

        String resultJson = JsonConverters.writeValueAsString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).delete(jsonPath).json();
    }

    private static void validate(Object object) throws DocumentApiException {
        if (object == null) throw DocumentApiException.toDocumentException(new AerospikeException(26));
    }
}
