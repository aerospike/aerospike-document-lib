package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;

public class JsonPathQuery {

    public static Object read(JsonPathObject jsonPathObject, Object object) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.read(resultJson, jsonPath);
    }

    public static Object set(JsonPathObject jsonPathObject, Object object, Object value) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        JSONArray keys = JsonPath.parse(resultJson).read(jsonPath);
        if (!keys.isEmpty()) {
            return JsonPath.parse(resultJson).set(jsonPath, value).json();
        }
        String key = jsonPath.substring(jsonPath.lastIndexOf(".") + 1);
        jsonPath = jsonPath.substring(0, jsonPath.lastIndexOf("."));
        return JsonPath.parse(resultJson).put(jsonPath, key, value).json();
    }

    public static Object append(JsonPathObject jsonPathObject, Object object, Object value) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).add(jsonPath, value).json();
    }

    public static Object delete(JsonPathObject jsonPathObject, Object object) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).delete(jsonPath).json();
    }
}
