package com.aerospike.documentapi.JsonPathQueryTests;

import com.aerospike.documentapi.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonPathWildcardTests extends BaseTestConfig {

    @Test
    public void testWildcardAfterDot() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        String jsonString = DebugUtils.readJSONFromAFile("src/test/resources/store.json");
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        Map<String, Object> jsonNodeAsMap  = JsonConverters.convertJsonNodeToMap(jsonNode);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonNode);

        String jsonPath = "$.store.*";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        Object expectedObject = jsonNodeAsMap.get("store");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.store.*";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = jsonNodeAsMap.get("store");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testWildcardInsideBrackets() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        String jsonString = DebugUtils.readJSONFromAFile("src/test/resources/store.json");
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonNode);

        String jsonPath = "$.store.book[*].author";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        Object expectedObject = JsonPath.read(jsonString, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }
}
