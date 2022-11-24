package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONArray;

public class JsonPathQuery {

    static final String DOCUMENT_ROOT = "$";

    private JsonPathQuery() {
    }

    public static Object read(JsonPathObject jsonPathObject, Object object) throws JsonProcessingException, JsonPathException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.read(resultJson, jsonPath);
    }

    public static Object putOrSet(JsonPathObject jsonPathObject, Object object, Object value) throws JsonProcessingException, JsonPathException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
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

    public static Object append(JsonPathObject jsonPathObject, Object object, Object value) throws JsonProcessingException, JsonPathException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).add(jsonPath, value).json();
    }

    public static Object delete(JsonPathObject jsonPathObject, Object object) throws JsonProcessingException, JsonPathException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = DOCUMENT_ROOT + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).delete(jsonPath).json();
    }
}
