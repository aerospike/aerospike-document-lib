package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;

public class JsonPathQuery {

    public static Object read(JsonPathObject jsonPathObject, Object object) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.read(resultJson, jsonPath);
    }

    public static Object set(JsonPathObject jsonPathObject, Object object, Object value) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).set(jsonPath, value).json();
    }

    public static Object delete(JsonPathObject jsonPathObject, Object object) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(object);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).delete(jsonPath).json();
    }
}
