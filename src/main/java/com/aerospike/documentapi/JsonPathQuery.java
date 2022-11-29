package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.JsonPathException;
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
        if (!keys.isEmpty()) {
            return JsonPath.parse(resultJson).set(jsonPath, value).json();
        }
        String key = jsonPath.substring(jsonPath.lastIndexOf(".") + 1);
        jsonPath = jsonPath.substring(0, jsonPath.lastIndexOf("."));
        return JsonPath.parse(resultJson).put(jsonPath, key, value).json();
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
