package com.aerospike.documentapi.JsonPathQueryTests;

import com.aerospike.documentapi.AerospikeDocumentClient;
import com.aerospike.documentapi.BaseTestConfig;
import com.aerospike.documentapi.JsonConverters;
import com.aerospike.documentapi.TestJsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonPathFilterTests extends BaseTestConfig {

    @BeforeEach
    void setUp() throws IOException {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);
    }

    @AfterEach
    void tearDown() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, "$");
    }

    @Test
    void testFilters() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String jsonPath;
        Object objectFromDB;
        Object expectedObject;

        // All books with an ISBN number
        jsonPath = "$..book[?(@.isbn)]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        // All books in store cheaper than 10
        jsonPath = "$.store.book[?(@.price < 10)]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        // All books in store that are not "expensive"
        jsonPath = "$..book[?(@.price <= $['expensive'])]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        // All books matching regex (ignore case)
        jsonPath = "$..book[?(@.author =~ /.*REES/i)]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testPutNonExistingKey() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String conditionValue = "new";
        String jsonPath = "$.store.book[?(@.title==\"The Lord of the Rings\")].condition";
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, conditionValue);

        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new String[]{conditionValue}, result.toArray());
    }

    @Test
    void testPutExistingKey() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String isbnValue = "0-111-22222-3";
        String jsonPath = "$.store.book[?(@.title==\"The Lord of the Rings\")].isbn";
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, isbnValue);

        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new String[]{isbnValue}, result.toArray());
    }
}
