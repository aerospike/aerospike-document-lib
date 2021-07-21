package com.aerospike.documentapi.JsonPathQueryTests;

import com.aerospike.documentapi.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonPathWildcardTests extends BaseTestConfig {

    @Test
    public void testWildcardAfterDot() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        Map<String, Object> jsonNodeAsMap  = JsonConverters.convertJsonNodeToMap(jsonNode);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // All things, both books and bicycles
        String jsonPath = "$.store.*";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object expectedObject = jsonNodeAsMap.get("store");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testWildcardInsideBrackets() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The authors of all books
        String jsonPath = "$.store.book[*].author";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object expectedObject = JsonPath.read(storeJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testWildcardInsideBracketsPut() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The authors of all books
        String jsonPath = "$.store.book[*].author";
        String jsonObject = "J.K. Rowling";
        // Modify the authors of all books to "J.K. Rowling"
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).set(jsonPath, jsonObject).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testWildcardInsideBracketsAppend() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The ref field of all books
        String jsonPath = "$.store.book[*].ref";
        Integer jsonObject = 999;
        // Add 999 at the end of the inner "ref" array of each book
        documentClient.append(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).add(jsonPath, jsonObject).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void testWildcardInsideBracketsDelete() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // The authors of all books
        String jsonPath = "$.store.book[*].author";
        // Modify the authors of all books to "J.K. Rowling"
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object modifiedJson = JsonPath.parse(storeJson).delete(jsonPath).json();
        Object expectedObject = JsonPath.read(modifiedJson, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    @Test
    public void deleteRootElementJSONPathQuery() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        String jsonPath = "$..*";
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertTrue(((List<?>) objectFromDB).isEmpty());
    }
}
