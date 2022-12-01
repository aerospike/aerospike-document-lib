package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.AerospikeDocumentClient;
import com.aerospike.documentapi.BaseTestConfig;
import com.aerospike.documentapi.util.JsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import net.minidev.json.JSONArray;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonPathFunctionsTests extends BaseTestConfig {

    @BeforeAll
    static void setUp() throws IOException {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(cdtJson);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);
    }

    @Test
    void testLength() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String jsonPath = "$.[*].x1.length()";
        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new Integer[]{5, 5, 5, 5}, result.toArray());

        jsonPath = "$.k3.length()";
        Object result2 = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertEquals(3, result2);
    }

    @Test
    void testMin() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String jsonPath = "$.[*].x1.min()";
        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new Double[]{1.0, 1.0, 1.0, 1.0}, result.toArray());

        jsonPath = "$.k2.min()";
        Object result2 = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertEquals(1.0, result2);
    }

    @Test
    void testMax() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String jsonPath = "$.[*].x1.max()";
        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new Double[]{5.0, 5.0, 5.0, 5.0}, result.toArray());

        jsonPath = "$.k2.max()";
        Object result2 = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertEquals(3.0, result2);
    }

    @Test
    void testAvg() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String jsonPath = "$.[*].x1.avg()";
        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new Double[]{3.0, 3.0, 3.0, 3.0}, result.toArray());

        jsonPath = "$.k2.avg()";
        Object result2 = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertEquals(2.0, result2);
    }

    @Test
    void testStddev() throws Exception {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String jsonPath = "$.[*].x1.stddev()";
        JSONArray result = (JSONArray) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertArrayEquals(new Double[]{1.4142135623730951, 1.4142135623730951, 1.4142135623730951, 1.4142135623730951},
                result.toArray());

        jsonPath = "$.k2.stddev()";
        Object result2 = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertEquals(0.8164965809277263, result2);
    }
}
