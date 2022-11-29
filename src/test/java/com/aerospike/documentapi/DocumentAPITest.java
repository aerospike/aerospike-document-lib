package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.documentapi.batch.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentAPITest extends BaseTestConfig {

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
    public void testPositivePathRetrieval() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);
        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);

        // Test different retrieval paths
        String jsonPath;
        Object objectFromDB;
        Object expectedObject;

        jsonPath = "$";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = jsonNodeAsMap;
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example1";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = jsonNodeAsMap.get("example1");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) jsonNodeAsMap.get("example3")).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key10";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example1.key01[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example1")).get("key01")).get(2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key09[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(1)).get("key09")).get(2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key10.key11";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((Map<?, ?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10")).get("key11");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key13.key15[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key13")).get("key15")).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[2].key21";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((Map<?, ?>) ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key19")).get(2)).get("key21");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[2].key20[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key19")).get(2)).get("key20")).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[2].key21.key23";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((Map<?, ?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(2)).get("key21")).get("key23");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key08[1].key16";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(1)).get("key08")).get(1)).get("key16");
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key08[1].key17[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) ((Map<?, ?>) ((List<?>) ((Map<?, ?>) ((List<?>) jsonNodeAsMap.get("example3")).get(1)).get("key08")).get(1)).get("key17")).get(2);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[3][1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        expectedObject = ((List<?>) ((List<?>) ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key19")).get(3)).get(1);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, expectedObject));
    }

    /**
     * Check correct response to all possible types of incorrect path:
     * <ul>
     * <li>Reference a list as if it were a map.</li>
     * <li>Reference a map as if it were a list.</li>
     * <li>Reference a primitive as if it was a map.</li>
     * <li>Reference a primitive as if it was a list.</li>
     * <li>Reference a list item that is not there (out of bounds).</li>
     * <li>Reference a map that isn't there.</li>
     * <li>Reference a list that isn't there.</li>
     * </ul>
     */
    @Test
    public void testNegativePathRetrieval() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different retrieval paths
        String jsonPath;
        Object objectFromDB = null;

        // Reference a list as if it were a map
        // $.example3[1].key08 is a list
        jsonPath = "$.example3[1].key08.nonexistentkey"; // returns error 4 - parameter error
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.KeyNotFoundException should have been thrown");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Reference a map as if it were a list
        // $.example1 is a map
        jsonPath = "$.example1[1]";
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.NotAListException should have been thrown");
        } catch (DocumentApiException.NotAListException ignored) {
        }

        // Reference a primitive as if it was a map
        // $.example4.key10.key11 is a primitive
        jsonPath = "$.example4.key10.key11.nonexistentkey";
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.KeyNotFoundException should have been thrown");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Reference a primitive as if it was a list
        // $.example4.key10.key11 is a primitive
        jsonPath = "$.example4.key10.key11[2]";
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.NotAListException should have been thrown");
        } catch (DocumentApiException.NotAListException ignored) {
        }

        // Reference a list item that is not there
        jsonPath = "$.example4.key13.key15[9]"; // error 26 - not applicable
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Reference a map that isn't there
        jsonPath = "$.example4.nosuchkey.nosuchkey"; // returns error 26
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Reference a list that isn't there
        jsonPath = "$.example4.nosuchkey[1]"; // returns error 26
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
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
    public void testPositivePut() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Putting a new key into an existing map
        jsonPath = "$.example1.key27";
        putValue = 77;
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, putValue));

        // Putting a new key into an existing array
        jsonPath = "$.example1.key01[10]";
        putValue = 78;
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, putValue));
    }

    /**
     * Check correct response to erroneous access.
     * <ul>
     * <li>Putting a key into a map that doesn't exist.</li>
     * <li>Putting a value into a list that doesn't exist.</li>
     * <li>Treating a map as if it were a list.</li>
     * <li>Treating a list as if it were a map.</li>
     * </ul>
     */

    /**
     * Check correct response to erroneous access
     * <p>
     * Putting a key into a map that doesn't exist
     * Putting a value into a list that doesn't exist
     * Treating a map as if it were a list
     * Treating a list as if it were a map
     */
    @Test
    public void testNegativePut() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Put a key into a map that doesn't exist
        // Should throw object not found exception
        jsonPath = "$.example9.key01";
        putValue = 79;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Access a list that doesn't exist
        jsonPath = "$.example9[2]";
        putValue = 80;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Treat a map as if it were a list
        jsonPath = "$.example2.key09";
        putValue = 81;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.KeyNotFoundException should have been thrown");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Treat a list as if it were a map
        jsonPath = "$.example1[1]";
        putValue = 82;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.KeyNotFoundException should have been thrown");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        jsonPath = "$.example1.key01[3]";
        int[] putValue2 = {79};
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue2);
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
    public void testPositiveAppend() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different append paths
        String jsonPath;
        int putValue;
        List<?> appendedList;

        // Appending to an array referenced by a key
        jsonPath = "$.example1.key01";
        putValue = 83;
//        documentClient.append(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
        documentClient.append(TEST_AEROSPIKE_KEY, Collections.singletonList(documentBinName), jsonPath, putValue);
        appendedList = ((List<?>) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath));
        // Check that the last element in the list we appended to is the value we added
        assertTrue(TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), putValue));

        // Appending to an array referenced by an index
        jsonPath = "$.example4.key19[3]";
        putValue = 84;
        documentClient.append(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
        appendedList = ((List<?>) documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath));
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
    public void testNegativeAppend() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Append to a list that doesn't exist
        // Should throw object not found exception
        jsonPath = "$.example9";
        putValue = 83;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Append to a map
        // Throws key not found exception
        jsonPath = "$.example1";
        putValue = 84;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.KeyNotFoundException should have been thrown");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Append to a primitive
        // Throws key not found exception
        jsonPath = "$.example2.key02";
        putValue = 85;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue);
            fail("DocumentApiException.ObjectNotFoundException should have been thrown");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Appending an array
        jsonPath = "$.example1.key01";
        int[] putValue2 = {79};
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonPath, putValue2);
            fail("IllegalArgumentException should have been thrown");
        } catch (IllegalArgumentException ignored) {
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
    public void testPositiveDelete() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different delete paths
        String jsonPath;
        Object originalObject;
        Object deletedObjectRead;

        // Delete a primitive using a map reference
        jsonPath = "$.example4.key10.key11";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof String);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete a primitive using a list reference
        jsonPath = "$.example3[0].key07[1]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof String);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
        assertTrue(deletedObjectRead == null | !deletedObjectRead.equals(originalObject));

        // Delete a map using a map reference
        jsonPath = "$.example4.key10";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof Map);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete a list using a map reference
        jsonPath = "$.example4.key13.key15";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof List);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete a map using a list reference
        jsonPath = "$.example2[1]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof Map);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        try {
            deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
            assertTrue(deletedObjectRead == null | !deletedObjectRead.equals(originalObject));
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Delete a list using a list reference
        jsonPath = "$.example4.key19[3]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object exists
        assertTrue(originalObject instanceof List);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        try {
            deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
            assertTrue(deletedObjectRead == null | !deletedObjectRead.equals(originalObject));
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
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
    public void testNegativeDelete() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        // Test different delete paths
        String jsonPath;
        Object originalObject;
        Object deletedObjectRead;

        // Delete a key in an existing map where key does not exist
        jsonPath = "$.example1.nokey";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the original object doesn't exist
        assertNull(originalObject);
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // This call will succeed
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        // Check the deleted object does not exist
        assertNull(deletedObjectRead);

        // Delete an out of range element in an existing list
        jsonPath = "$.example2[3]";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Delete a key in a list
        jsonPath = "$.example2.nokey";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Delete an index in a map
        jsonPath = "$.example1[1]";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.NotAListException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.KeyNotFoundException ignored) {
        }

        // Delete a key in a map that doesn't exist
        jsonPath = "$.nokey.nokey";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Delete an index in a list that doesn't exist
        jsonPath = "$.nolist[1]";
        // accessing this path throws an error, so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
            fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
        }
    }

    @Test
    public void testTopLevelArrayType() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(topLevelArrayTypeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);
        List<Object> jsonNodeAsList = JsonConverters.convertJsonNodeToList(jsonNode);

        String jsonPath = "$";
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);

        assertTrue(TestJsonConverters.jsonEquals(objectFromDB, jsonNodeAsList));
    }

    @Test
    public void deleteRootElement() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        String jsonPath = "$";
        documentClient.delete(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertTrue(((Map<?, ?>) objectFromDB).isEmpty());
    }
}
