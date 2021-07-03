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
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The price of everything
        String jsonPath = "$.store..price";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testDeepScanAtTheBeginning() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The third book
        String jsonPath = "$..book[2]";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testDeepScanWithWildCard() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Give me every thing
        String jsonPath = "$..*";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testDeepScanPut() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Give me every thing
        String jsonPath = "$..author";
        String jsonObject = "J.K. Rowling";
        // Modify the authors of all books to "J.K. Rowling"
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).set(jsonPath, jsonObject).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testDeepScanDelete() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The price of everything
        String jsonPath = "$.store..price";
        // Delete the price field of every object exists in the store
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).delete(jsonPath).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }
}
