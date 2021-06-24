package com.aerospike.documentapi.JsonPathQueryTests;

import com.aerospike.documentapi.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonPathDeepScanTests extends BaseTestConfig {

    @Test
    public void testDeepScan() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonNode);

        String jsonPath = "$.store..price";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testDeepScanAtTheBeginning() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonNode);

        String jsonPath = "$..book[2]";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }
}
