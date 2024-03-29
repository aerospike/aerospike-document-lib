package com.aerospike.documentapi;

import com.aerospike.documentapi.util.JsonConverters;
import com.aerospike.documentapi.util.TestJsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultipleBinsTests extends BaseTestConfig {

    @Test
    void testMultipleGetOperations() {
        JsonNode jsonNodeEvents1 = JsonConverters.convertStringToJsonNode(events1);
        JsonNode jsonNodeEvents2 = JsonConverters.convertStringToJsonNode(events2);
        Map<String, Object> jsonNodeAsMapEvents1 = JsonConverters.convertJsonNodeToMap(jsonNodeEvents1);
        Map<String, Object> jsonNodeAsMapEvents2 = JsonConverters.convertJsonNodeToMap(jsonNodeEvents2);
        String documentBinName1 = "events1Bin";
        String documentBinName2 = "events2Bin";
        List<String> bins = new ArrayList<>();
        bins.add(documentBinName1);
        bins.add(documentBinName2);

        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName1, jsonNodeEvents1);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName2, jsonNodeEvents2);
        String jsonPath = "$.authentication.logout.name";

        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);
        Object expectedObject1 =
                ((Map<?, ?>) ((Map<?, ?>) jsonNodeAsMapEvents1.get("authentication")).get("logout")).get("name");
        Object expectedObject2 =
                ((Map<?, ?>) ((Map<?, ?>) jsonNodeAsMapEvents2.get("authentication")).get("logout")).get("name");
        Map<String, Object> expectedObjectsCombined = new HashMap<>();
        expectedObjectsCombined.put(documentBinName1, expectedObject1);
        expectedObjectsCombined.put(documentBinName2, expectedObject2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObjectsCombined));
    }

    @Test
    void testMultiplePutOperations() {
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
        String jsonPath = "$.authentication.logout.device";
        String putValue = "Computer";

        documentClient.put(TEST_AEROSPIKE_KEY, bins, jsonPath, putValue);

        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);
        Map<String, Object> expectedObjectsCombined = new HashMap<>();
        expectedObjectsCombined.put(documentBinName1, putValue);
        expectedObjectsCombined.put(documentBinName2, putValue);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObjectsCombined));
    }

    @Test
    void testMultipleAppendOperations() {
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
        String jsonPath = "$.authentication.logout.ref";
        int putValue = 12;

        documentClient.append(TEST_AEROSPIKE_KEY, bins, jsonPath, putValue);

        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

        List<?> appendedList1 = ((List<?>) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName1, jsonPath));
        List<?> appendedList2 = ((List<?>) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName2, jsonPath));
        Map<String, Object> expectedObjectsCombined = new HashMap<>();
        expectedObjectsCombined.put(documentBinName1, appendedList1);
        expectedObjectsCombined.put(documentBinName2, appendedList2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObjectsCombined));
    }

    @Test
    void testMultipleDeleteOperations() {
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
        String jsonPath = "$.authentication.logout.device";

        documentClient.delete(TEST_AEROSPIKE_KEY, bins, jsonPath);

        Map<?, ?> objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);
        for (Object value : objectFromDB.values()) {
            assertNull(value);
        }
    }

    @Test
    void deleteRootElementMultipleBins() {
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

        String jsonPath = "$";
        documentClient.delete(TEST_AEROSPIKE_KEY, bins, jsonPath);
        Map<?, ?> objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, bins, jsonPath);

        objectFromDB.values().forEach(value -> assertTrue(((Map<?, ?>) value).isEmpty()));
    }
}
