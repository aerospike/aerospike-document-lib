package com.aerospike.documentapi.JsonPathQueryTests;

import com.aerospike.documentapi.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonPathMultipleBinsTests extends BaseTestConfig {

    @Test
    public void testMultipleBinsWithJsonPathQueries() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNodeEvents1 = JsonConverters.convertStringToJsonNode(events1);
        JsonNode jsonNodeEvents2 = JsonConverters.convertStringToJsonNode(events2);
        String documentBinName1 = "events1Bin";
        String documentBinName2 = "events2Bin";
        List<String> bins = new ArrayList<>();
        bins.add(documentBinName1);
        bins.add(documentBinName2);

        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName1, jsonNodeEvents1);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName2, jsonNodeEvents2);

        String jsonPath = "$.authentication.login[*].os";
        String jsonObject = "Windows";

        // Modify the os of all the logins to "Windows"
        documentClient.put(TEST_AEROSPIKE_KEY, bins, jsonPath, jsonObject);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

        Object modifiedJson1 = JsonPath.parse(events1).set(jsonPath, jsonObject).json();
        Object expectedObject1 = JsonPath.read(modifiedJson1, jsonPath);
        Object modifiedJson2 = JsonPath.parse(events2).set(jsonPath, jsonObject).json();
        Object expectedObject2 = JsonPath.read(modifiedJson2, jsonPath);

        Map<String, Object> expectedObjectsCombined = new HashMap<>();
        expectedObjectsCombined.put(documentBinName1, expectedObject1);
        expectedObjectsCombined.put(documentBinName2, expectedObject2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObjectsCombined));

        jsonPath = "$.authentication..device";
        jsonObject = "Mobile";

        // Modify the devices of all the authentications (login and logout) to "Mobile"
        documentClient.put(TEST_AEROSPIKE_KEY, bins, jsonPath, jsonObject);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

        modifiedJson1 = JsonPath.parse(events1).set(jsonPath, jsonObject).json();
        expectedObject1 = JsonPath.read(modifiedJson1, jsonPath);
        modifiedJson2 = JsonPath.parse(events2).set(jsonPath, jsonObject).json();
        expectedObject2 = JsonPath.read(modifiedJson2, jsonPath);

        expectedObjectsCombined.clear();
        expectedObjectsCombined.put(documentBinName1, expectedObject1);
        expectedObjectsCombined.put(documentBinName2, expectedObject2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObjectsCombined));

        jsonPath = "$.authentication.login[?(@.id > 10)]";

        client.delete(null, TEST_AEROSPIKE_KEY);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName1, jsonNodeEvents1);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName2, jsonNodeEvents2);

        // All the logins with "id" greater than 10
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

        expectedObject1 = JsonPath.read(events1, jsonPath);
        expectedObject2 = JsonPath.read(events2, jsonPath);

        expectedObjectsCombined.clear();
        expectedObjectsCombined.put(documentBinName1, expectedObject1);
        expectedObjectsCombined.put(documentBinName2, expectedObject2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObjectsCombined));
    }

    @Test
    public void deleteRootElementMultipleBinsWithJSONPathQuery() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNodeEvents1 = JsonConverters.convertStringToJsonNode(events1);
        JsonNode jsonNodeEvents2 = JsonConverters.convertStringToJsonNode(events2);
        String documentBinName1 = "events1Bin";
        String documentBinName2 = "events2Bin";
        List<String> bins = new ArrayList<>();
        bins.add(documentBinName1);
        bins.add(documentBinName2);

        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName1, jsonNodeEvents1);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName2, jsonNodeEvents2);

        String jsonPath = "$..*";
        documentClient.delete(TEST_AEROSPIKE_KEY, bins, jsonPath);
        Map<?, ?> objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

        objectFromDB.values().forEach(valueList -> assertTrue(((List<?>) valueList).isEmpty()));
    }
}
