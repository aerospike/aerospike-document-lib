package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;

public class JsonPathQuery {

    public static Object read(JsonPathObject jsonPathObject, Object result) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(result);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.read(resultJson, jsonPath);
    }

    public static Object set(JsonPathObject jsonPathObject, Object result, Object value) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(result);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.parse(resultJson).set(jsonPath, value).json();
    }
}
