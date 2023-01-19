package com.aerospike.documentapi;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.util.JsonConverters;
import com.aerospike.documentapi.util.Lut;
import com.aerospike.documentapi.util.TestJsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mockStatic;

class DocumentAPITests extends BaseTestConfig {

    /**
     * Check that the following paths will correctly retrieve document content when content exists:
     * <ul>
     * <li>.</li>
     * <li>.key1</li>
     * <li>.key1[i]</li>
     * <li>.key1.key2</li>
     * <li>.key1.key2[i]</li>
     * <li>.key1[i].key2</li>
     * <li>.key1[i].key2[j]</li>
     * <li>.key1.key2.key3</li>
     * <li>.key1.key2.key3[i]</li>
     * <li>.key1.key2[i].key3</li>
     * <li>.key1.key2[i].key3[j]</li>
     * <li>.key1[i].key2.key3</li>
     * <li>.key1[i].key2.key3[j]</li>
     * <li>.key1[i].key2[j].key3</li>
     * <li>.key1[i].key2[j].key3[k]</li>
     * <li>.key1.key2[i][j]</li>
     * </ul>
     */
    @Test
    void testPositivePathRetrieval() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);
        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);

        // Test different retrieval paths
        String jsonPath;
        Object objectFromDB;
        Object expectedObject;

        jsonPath = "$";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = jsonNodeAsMap;
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example1";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = jsonNodeAsMap.get("example1");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) jsonNodeAsMap.get("example3")).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key10";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example1.key01[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example1")).get("key01")).get(2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key09[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(1)).get("key09")).get(2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key10.key11";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((Map<?, ?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10")).get("key11");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key13.key15[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key13")).get("key15")).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[2].key21";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((Map<?, ?>) ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key19")).get(2)).get("key21");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[2].key20[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key19"))
                .get(2)).get("key20")).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[2].key21.key23";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((Map<?, ?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(2)).get("key21")).get("key23");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key08[1].key16";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(1))
                .get("key08")).get(1)).get("key16");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key08[1].key17[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3"))
                .get(1)).get("key08")).get(1)).get("key17")).get(2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[3][1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        expectedObject = ((List<?>) ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key19")).get(3)).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    /**
     * Check that the client can be used to read json with keys of type long and binary data as List values
     */
    @Test
    void testIrregularJsonRetrieval() {
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        Map<Long, List<Map<Long, Map<String, byte[]>>>> map = new HashMap<>();
        String mapKey = "A1";
        String testMapValue = "This is test1 value ☺";
        Map<String, byte[]> insideMap = new HashMap<>();
        insideMap.put(mapKey, testMapValue.getBytes(StandardCharsets.UTF_8));
        Map<Long, Map<String, byte[]>> innerMap = new HashMap<>();
        innerMap.put(3L, insideMap);
        List<Map<Long, Map<String, byte[]>>> list = new ArrayList<>();
        list.add(innerMap);
        map.put(2L, list);

        // Load the incorrect "json" map
        writeDocumentToDB(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, map);

        String jsonPath = "$";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);

        assertNotNull(objectFromDB);
        assertTrue(isValidInnerMapElement(objectFromDB, mapKey, testMapValue));
    }

    private boolean isValidInnerMapElement(Object objectFromDB, String mapKey, String testMapValue) {
        @SuppressWarnings("unchecked")
        byte[] res = ((Map<Long, List<Map<Long, Map<String, byte[]>>>>) objectFromDB)
                .get(2L).get(0).get(3L).get(mapKey);

        return new String(res, StandardCharsets.UTF_8).equals(testMapValue);
    }

    /**
     * Check correct response to all possible types of incorrect path:
     * <ul>
     * <li>Reference a list as if it were a map.</li>
     * <li>Reference a map as if it were a list.</li>
     * <li>Reference a primitive as if it were a map.</li>
     * <li>Reference a primitive as if it were a list.</li>
     * <li>Reference a list item that is not there (out of bounds).</li>
     * <li>Reference a map that isn't there.</li>
     * <li>Reference a list that isn't there.</li>
     * </ul>
     */
    @Test
    void testNegativePathRetrieval() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different retrieval paths
        String jsonPath;

        // Reference a list as if it were a map
        // $.example3[1].key08 is a list
        jsonPath = "$.example3[1].key08.nonexistentkey";
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Reference a map as if it were a list
        // $.example1 is a map
        jsonPath = "$.example1[1]";
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Reference a primitive as if it were a map
        // $.example4.key10.key11 is a primitive
        jsonPath = "$.example4.key10.key11.nonexistentkey";
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Reference a primitive as if it were a list
        // $.example4.key10.key11 is a primitive
        jsonPath = "$.example4.key10.key11[2]";
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Reference a list item that is not there
        jsonPath = "$.example4.key13.key15[9]";
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Reference a map that isn't there
        jsonPath = "$.example4.nosuchkey.nosuchkey"; // returns error 26
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Reference a list that isn't there
        jsonPath = "$.example4.nosuchkey[1]";
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Make sure 'good' puts give correct result.
     * <ul>
     * <li>Putting a key into an existing map.</li>
     * <li>Putting a value into an existing list.</li>
     * </ul>
     */
    @Test
    void testPositivePut() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Putting a new key into an existing map
        jsonPath = "$.example1.key27";
        putValue = 77;
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, putValue));

        // Putting a new key into an existing array
        jsonPath = "$.example1.key01[10]";
        putValue = 78;
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, putValue));
    }

    @Test
    void testAtomicPut() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(events1);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        try (MockedStatic<Lut> classMock = mockStatic(Lut.class)) {
            classMock.when(() -> Lut.setLutPolicy(ArgumentMatchers.<WritePolicy>any(), anyLong()))
                    .thenReturn(mockLutWritePolicy());

            final String jsonPath = "$.authentication..id";
            final int putValue = 77;
            DocumentApiException e = assertThrows(
                    DocumentApiException.class,
                    () -> documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue)
            );
            assertTrue(e.getMessage().contains("Transaction filtered out"));

            final String jsonPath2 = "$.authentication..ref[1]";
            final int putValue2 = 78;
            DocumentApiException e2 = assertThrows(
                    DocumentApiException.class,
                    () -> documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath2, putValue2)
            );
            assertTrue(e2.getMessage().contains("Transaction filtered out"));
        }
    }

    /**
     * Check correct response to erroneous access.
     * <ul>
     * <li>Putting a key into a map that doesn't exist.</li>
     * <li>Putting a value into a list that doesn't exist.</li>
     * <li>Treating a list as if it were a map.</li>
     * <li>Treating a map as if it were a list.</li>
     * <li>Putting unacceptable data type (an array instead of a list).</li>
     * </ul>
     */
    @Test
    void testNegativePut() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different put paths
        String jsonPath;
        int putValue;

        // Put a key into a map that doesn't exist
        // Should throw object not found exception
        jsonPath = "$.example9.key01";
        putValue = 79;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Put a key into a list that doesn't exist
        jsonPath = "$.example9[2]";
        putValue = 80;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Treat a list (example2) as if it were a map
        jsonPath = "$.example2.key09";
        putValue = 81;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Treat a map as if it were a list
        jsonPath = "$.example1[1]";
        putValue = 82;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Put unacceptable data type (an array instead of a list)
        jsonPath = "$.example1.key01[3]";
        int[] putValue2 = {79};
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue2);
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException ignored) {
        }
    }

    /**
     * Make sure 'good' appends give correct result.
     * <ul>
     * <li>Appending to a list referenced using a key.</li>
     * <li>Appending to a list referenced by an index.</li>
     * </ul>
     */
    @Test
    void testPositiveAppend() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different append paths
        String jsonPath;
        int putValue;
        List<?> appendedList;

        // Appending to an array referenced by a key
        jsonPath = "$.example1.key01";
        putValue = 83;
        documentClient.append(TEST_AEROSPIKE_KEY, Collections.singletonList(DOCUMENT_BIN_NAME), jsonPath, putValue);
        appendedList = ((List<?>) documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath));
        // Check that the last element in the list we appended to is the value we added
        assertTrue(TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), putValue));

        // Appending to an array referenced by an index
        jsonPath = "$.example4.key19[3]";
        putValue = 84;
        documentClient.append(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
        appendedList = ((List<?>) documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath));
        // Check that the last element in the list we appended to is the value we added
        assertTrue(TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), putValue));
    }

    /**
     * Make sure erroneous appends handled correctly.
     * <ul>
     * <li>Appending to a list that doesn't exist.</li>
     * <li>Appending to a map.</li>
     * <li>Appending to a primitive.</li>
     * </ul>
     */
    @Test
    void testNegativeAppend() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different put paths
        String jsonPath;
        int putValue;

        // Append to a list that doesn't exist
        // Should throw object not found exception
        jsonPath = "$.example9";
        putValue = 83;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Append to a map
        // Throws key not found exception
        jsonPath = "$.example1";
        putValue = 84;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Append to a primitive
        // Throws key not found exception
        jsonPath = "$.example2.key02";
        putValue = 85;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue);
            fail("DocumentApiException should have been thrown");
        } catch (DocumentApiException ignored) {
        }

        // Appending an array
        jsonPath = "$.example1.key01";
        int[] putValue2 = {79};
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue2);
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException ignored) {
        }
    }

    @Test
    void testAtomicAppend() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(events1);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        try (MockedStatic<Lut> classMock = mockStatic(Lut.class)) {
            classMock.when(() -> Lut.setLutPolicy(ArgumentMatchers.<WritePolicy>any(), anyLong()))
                    .thenReturn(mockLutWritePolicy());

            final String jsonPath = "$.authentication..ref";
            final int putValue = 78;
            DocumentApiException e = assertThrows(
                    DocumentApiException.class,
                    () -> documentClient.append(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath, putValue)
            );
            assertTrue(e.getMessage().contains("Transaction filtered out"));
        }
    }

    /**
     * Make sure 'good' deletes give correct result.
     * <ul>
     * <li>Delete a primitive using a map reference.</li>
     * <li>Delete a primitive using a list reference.</li>
     * <li>Delete a map using a map reference.</li>
     * <li>Delete a list using a map reference.</li>
     * <li>Delete a map using a list reference.</li>
     * <li>Delete a list using a list reference.</li>
     * </ul>
     */
    @Test
    void testPositiveDelete() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different delete paths
        String jsonPath;
        Object originalObject;
        Object deletedObjectRead;

        // Delete a primitive using a map reference
        jsonPath = "$.example4.key10.key11";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof String);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete a primitive using a list reference
        jsonPath = "$.example3[0].key07[1]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof String);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
        assertTrue(deletedObjectRead == null || !deletedObjectRead.equals(originalObject));

        // Delete a map using a map reference
        jsonPath = "$.example4.key10";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof Map);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete a list using a map reference
        jsonPath = "$.example4.key13.key15";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof List);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete a map using a list reference
        jsonPath = "$.example2[1]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof Map);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        try {
            deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
            assertTrue(deletedObjectRead == null || !deletedObjectRead.equals(originalObject));
        } catch (DocumentApiException ignored) {
        }

        // Delete a list using a list reference
        jsonPath = "$.example4.key19[3]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof List);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        try {
            deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
            assertTrue(deletedObjectRead == null || !deletedObjectRead.equals(originalObject));
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Make sure 'bad' deletes are handled correctly.
     * <ul>
     * <li>Delete a key in an existing map where key does not exist.</li>
     * <li>Delete an out of range element in an existing list.</li>
     * <li>Delete a key in a list.</li>
     * <li>Delete an index in a map.</li>
     * <li>Delete a key in a map that doesn't exist.</li>
     * <li>Delete an index in a list that doesn't exist.</li>
     * </ul>
     */
    @Test
    void testNegativeDelete() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        // Test different delete paths
        String jsonPath;
        Object originalObject;
        Object deletedObjectRead;

        // Delete a key in an existing map where key does not exist
        jsonPath = "$.example1.nokey";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the original object doesn't exist
        assertNull(originalObject);
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // This call will succeed
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete an out of range element in an existing list
        jsonPath = "$.example2[3]";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete a key in a list
        jsonPath = "$.example2.nokey";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete an index in a map
        jsonPath = "$.example1[1]";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete a key in a map that doesn't exist
        jsonPath = "$.nokey.nokey";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete an index in a list that doesn't exist
        jsonPath = "$.nolist[1]";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException ignored) {
        }
    }

    @Test
    void testTopLevelArrayType() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(topLevelArrayTypeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);
        List<Object> jsonNodeAsList = JsonConverters.convertJsonNodeToList(jsonNode);

        String jsonPath = "$";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);

        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, jsonNodeAsList));
    }

    @Test
    void deleteRootElement() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        String jsonPath = "$";
        documentClient.delete(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        assertTrue(((Map<?, ?>) objectFromDB).isEmpty());
    }

    private WritePolicy mockLutWritePolicy() {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.filterExp = Exp.build(
                Exp.eq(
                        Exp.lastUpdate(),
                        Exp.val(1234L)
                )
        );
        writePolicy.failOnFilteredOut = true;
        return writePolicy;
    }
}
