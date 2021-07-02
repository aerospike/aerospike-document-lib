package com.aerospike.documentapi.JsonPathQueryTests;

import com.aerospike.documentapi.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonPathFilterTests extends BaseTestConfig {

    @Test
    public void testFilters() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonNode, documentBinName);

        String jsonPath;
        Object objectFromDB;
        Object expectedObject;

        // All books with an ISBN number
        jsonPath = "$..book[?(@.isbn)]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath, documentBinName);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        // All books in store cheaper than 10
        jsonPath = "$.store.book[?(@.price < 10)]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath, documentBinName);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        // All books in store that are not "expensive"
        jsonPath = "$..book[?(@.price <= $['expensive'])]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath, documentBinName);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        // All books matching regex (ignore case)
        jsonPath = "$..book[?(@.author =~ /.*REES/i)]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath, documentBinName);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }
}
