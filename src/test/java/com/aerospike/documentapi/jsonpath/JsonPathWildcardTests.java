package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.AerospikeDocumentClient;
import com.aerospike.documentapi.BaseTestConfig;
import com.aerospike.documentapi.util.JsonConverters;
import com.aerospike.documentapi.util.TestJsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonPathWildcardTests extends BaseTestConfig {

    @Test
    void testWildcardAfterDot() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // All things, both books and bicycles
        String jsonPath = "$.store.*";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object expectedObject = jsonNodeAsMap.get("store");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testWildcardInsideBrackets() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The authors of all books
        String jsonPath = "$.store.book[*].author";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testWildcardInsideBracketsPut() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The authors of all books
        String jsonPath = "$.store.book[*].author";
        String jsonObject = "J.K. Rowling";
        // Modify the authors of all books to "J.K. Rowling"
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).set(jsonPath, jsonObject).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testWildcardInsideBracketsAppend() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The ref field of all books
        String jsonPath = "$.store.book[*].ref";
        Integer jsonObject = 999;
        // Add 999 at the end of the inner "ref" array of each book
        documentClient.append(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).add(jsonPath, jsonObject).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void testWildcardInsideBracketsDelete() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // The authors of all books
        String jsonPath = "$.store.book[*].author";
        // Delete the author field of every book in the store
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).delete(jsonPath).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    void deleteRootElementJSONPathQuery() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        String jsonPath = "$..*";
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        assertTrue(((List<?>) objectFromDB).isEmpty());
    }
}
