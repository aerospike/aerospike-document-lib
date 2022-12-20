package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.AerospikeDocumentClient;
import com.aerospike.documentapi.BaseTestConfig;
import com.aerospike.documentapi.util.JsonConverters;
import com.aerospike.documentapi.util.TestJsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonPathDeepScanTests extends BaseTestConfig {

    @Test
    void testDeepScan() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The price of everything
        String jsonPath = "$.store..price";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testDeepScanAtTheBeginning() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The third book
        String jsonPath = "$..book[2]";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testDeepScanWithWildCard() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Give me every thing
        String jsonPath = "$..*";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testDeepScanPut() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Give me every thing
        String jsonPath = "$..author";
        String jsonObject = "J.K. Rowling";
        // Modify the authors of all books to "J.K. Rowling"
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).set(jsonPath, jsonObject).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testDeepScanDelete() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The price of everything
        String jsonPath = "$.store..price";
        // Delete the price field of every object exists in the store
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).delete(jsonPath).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }
}
