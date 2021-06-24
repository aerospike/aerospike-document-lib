package com.aerospike.documentapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;

public class JsonPathQuery {
    public static Object execute(JsonPathObject jsonPathObject, Object result) throws JsonProcessingException {
        String resultJson = JsonConverters.convertObjectToJsonString(result);
        String jsonPath = "$" + jsonPathObject.getJsonPathSecondStepQuery();
        return JsonPath.read(resultJson, jsonPath);
    }
}
