package com.aerospike.documentAPI;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class DocumentAPITest extends BaseTestConfig {

    /**
     *
     * Check that the following paths will correctly retrieve document content when content exists
     * .
     * .key1
     * .key1[i]
     * .key1.key2
     * .key1.key2[i]
     * .key1[i].key2
     * .key1[i].key2[j]
     * .key1.key2.key3
     * .key1.key2.key3[i]
     * .key1.key2[i].key3
     * .key1.key2[i].key3[j]
     * .key1[i].key2.key3
     * .key1[i].key2.key3[j]
     * .key1[i].key2[j].key3
     * .key1[i].key2[j].key3[k]
     * .key1.key2[i][j]
     */
    @Test
    public void testPositivePathRetrieval() throws IOException,
            JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException {
        // Load the test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different retrieval paths
        String jsonPath;
        Object objectFromDB;
        Object expectedObject;

        jsonPath = "$";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = jsonAsMap;
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example1";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = jsonAsMap.get("example1");
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)jsonAsMap.get("example3")).get(1);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key10";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((Map)jsonAsMap.get("example4")).get("key10");
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example1.key01[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)((Map)jsonAsMap.get("example1")).get("key01")).get(2);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key09[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)((Map)((List)jsonAsMap.get("example3")).get(1)).get("key09")).get(2);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key10.key11";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((Map)((Map)jsonAsMap.get("example4")).get("key10")).get("key11");
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key13.key15[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)((Map)((Map)jsonAsMap.get("example4")).get("key13")).get("key15")).get(1);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[2].key21";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((Map)((List)((Map)jsonAsMap.get("example4")).get("key19")).get(2)).get("key21");
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[2].key20[1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)((Map)((List)((Map)jsonAsMap.get("example4")).get("key19")).get(2)).get("key20")).get(1);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[2].key21.key23";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((Map)((Map)((List)jsonAsMap.get("example3")).get(2)).get("key21")).get("key23");
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key08[1].key16";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((Map)((List)((Map)((List)jsonAsMap.get("example3")).get(1)).get("key08")).get(1)).get("key16");
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example3[1].key08[1].key17[2]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)((Map)((List)((Map)((List)jsonAsMap.get("example3")).get(1)).get("key08")).get(1)).get("key17")).get(2);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));

        jsonPath = "$.example4.key19[3][1]";
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        expectedObject = ((List)((List)((Map)jsonAsMap.get("example4")).get("key19")).get(3)).get(1);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, expectedObject));
    }

    /**
     * Check correct response to all possible types of incorrect path
     *
     * Reference a list as if it were a map
     * Reference a map as if it were a list
     * Reference a primitive as if it was a map
     * Reference a primitive as if it was a list
     * Reference a list item that is not there (out of bounds)
     * Reference a map that isn't there
     * Reference a list that isn't there
     */
    @Test
    public void testNegativePathRetrieval() throws IOException, JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException
    {
        // Load the test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different retrieval paths
        String jsonPath;
        Object objectFromDB = null;

        // Reference a list as if it were a map
        // $.example3[1].key08 is a list
        jsonPath = "$.example3[1].key08.nonexistentkey"; // returns error 4 - parameter error
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.KeyNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Reference a map as if it were a list
        // $.example1 is a map
        jsonPath = "$.example1[1]";
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.NotAListException should have been thrown");
        } catch (AerospikeDocumentClient.NotAListException ignored) {}

        // Reference a primitive as if it was a map
        // $.example4.key10.key11 is a primitive
        jsonPath = "$.example4.key10.key11.nonexistentkey";
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.KeyNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Reference a primitive as if it was a list
        // $.example4.key10.key11 is a primitive
        jsonPath = "$.example4.key10.key11[2]";
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.NotAListException should have been thrown");
        } catch (AerospikeDocumentClient.NotAListException ignored) {}

        // Reference a list item that is not there
        jsonPath = "$.example4.key13.key15[9]"; // error 26 - not applicable
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Reference a map that isn't there
        jsonPath = "$.example4.nosuchkey.nosuchkey"; // returns error 26
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Reference a list that isn't there
        jsonPath = "$.example4.nosuchkey[1]"; // returns error 26
        try {
            objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}
    }

    /**
     * Make sure 'good' puts give correct result
     *
     * Putting a key into an existing map
     * Putting a value into an existing list
     */
    @Test
    public void testPositivePut() throws IOException,
    JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException{
        // Set up test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Putting a new key into an existing map
        jsonPath = "$.example1.key27";
        putValue = 77;
        documentClient.put(TEST_AEROSPIKE_KEY, jsonPath, putValue);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, putValue));

        // Putting a new key into an existing array
        jsonPath = "$.example1.key01[10]";
        putValue = 78;
        documentClient.put(TEST_AEROSPIKE_KEY, jsonPath, putValue);
        objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        Assert.assertTrue(TestUtils.jsonEquals(objectFromDB, putValue));
    }

    /**
     * Check correct response to erroneous access
     *
     * Putting a key into a map that doesn't exist
     * Putting a value into a list that doesn't exist
     * Treating a map as if it were a list
     * Treating a list as if it were a map
     */
    @Test
    public void testNegativePut() throws IOException,
            JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException{
        // Load the test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Put a key into a map that doesn't exist
        // Should throw object not found exception
        jsonPath = "$.example9.key01";
        putValue = 79;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Access a list that doesn't exist
        jsonPath = "$.example9[2]";
        putValue = 80;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Treat a map as if it were a list
        jsonPath = "$.example2.key09";
        putValue = 81;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.KeyNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Treat a list as if it were a map
        jsonPath = "$.example1[1]";
        putValue = 82;
        try {
            documentClient.put(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.KeyNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}
    }

    /**
     * Make sure 'good' appends give correct result
     *
     * Appending to a list referenced using a key
     * Appending to a list referenced by an index
     */
    @Test
    public void testPositiveAppend() throws IOException,
            JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException {
        // Set up test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different append paths
        String jsonPath;
        int putValue;
        List appendedList;

        // Appending to an array referenced by a key
        jsonPath = "$.example1.key01";
        putValue = 83;
        documentClient.append(TEST_AEROSPIKE_KEY, jsonPath, putValue);
        appendedList = ((List)documentClient.get(TEST_AEROSPIKE_KEY, jsonPath));
        // Check that the last element in the list we appended to is the value we added
        Assert.assertTrue(TestUtils.jsonEquals(appendedList.get(appendedList.size() -1), putValue));

        // Appending to an array referenced by an index
        jsonPath = "$.example4.key19[3]";
        putValue = 84;
        documentClient.append(TEST_AEROSPIKE_KEY, jsonPath, putValue);
        appendedList = ((List)documentClient.get(TEST_AEROSPIKE_KEY, jsonPath));
        // Check that the last element in the list we appended to is the value we added
        Assert.assertTrue(TestUtils.jsonEquals(appendedList.get(appendedList.size() -1), putValue));
    }

    /**
     * Make sure erroneous appends handled correctly
     *
     * Appending to a list that doesn't exist
     * Appending to a map
     * Appending to a primitive
     */
    @Test
    public void testNegativeAppend() throws IOException,
            JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException {
        // Load the test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different put paths
        String jsonPath;
        int putValue;
        Object objectFromDB;

        // Append to a list that doesn't exist
        // Should throw object not found exception
        jsonPath = "$.example9";
        putValue = 83;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Append to a map
        // Throws key not found exception
        jsonPath = "$.example1";
        putValue = 84;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.KeyNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Append to a primitive
        // Throws key not found exception
        jsonPath = "$.example2.key02";
        putValue = 85;
        try {
            documentClient.append(TEST_AEROSPIKE_KEY, jsonPath, putValue);
            Assert.fail("AerospikeDocumentClient.ObjectNotFoundException should have been thrown");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}
    }

    /**
     * Make sure 'good' deletes give correct result
     *
     * Delete a primitive using a map reference
     * Delete a primitive using a list reference
     * Delete a map using a map reference
     * Delete a list using a map reference
     * Delete a map using a list reference
     * Delete a list using a list reference
     *
     */
    @Test
    public void testPositiveDelete() throws IOException,
            JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException {
        // Set up test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different delete paths
        String jsonPath;
        Object originalObject;
        Object deletedObjectRead;

        // Delete a primitive using a map reference
        jsonPath = "$.example4.key10.key11";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object exists
        Assert.assertTrue(originalObject instanceof String);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the deleted object does not exist
        Assert.assertNull(deletedObjectRead);

        // Delete a primitive using a list reference
        jsonPath = "$.example3[0].key07[1]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object exists
        Assert.assertTrue(originalObject instanceof String);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
        Assert.assertTrue(deletedObjectRead == null | ! deletedObjectRead.equals(originalObject));

        // Delete a map using a map reference
        jsonPath = "$.example4.key10";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object exists
        Assert.assertTrue(originalObject instanceof Map);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the deleted object does not exist
        Assert.assertNull(deletedObjectRead);

        // Delete a list using a map reference
        jsonPath = "$.example4.key13.key15";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object exists
        Assert.assertTrue(originalObject instanceof List);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the deleted object does not exist
        Assert.assertNull(deletedObjectRead);

        // Delete a map using a list reference
        jsonPath = "$.example2[1]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object exists
        Assert.assertTrue(originalObject instanceof Map);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        try {
            deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
            Assert.assertTrue(deletedObjectRead == null | !deletedObjectRead.equals(originalObject));
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Delete a list using a list reference
        jsonPath = "$.example4.key19[3]";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object exists
        Assert.assertTrue(originalObject instanceof List);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        try {
            deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            // Check the deleted object does not exist, or that we now have a different object (possible in a list delete)
            Assert.assertTrue(deletedObjectRead == null | !deletedObjectRead.equals(originalObject));
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}
    }

    /**
     * Make sure 'bad' deletes are handled correctly
     *
     * Delete a key in an existing map where key does not exist
     * Delete an out of range element in an existing list
     * Delete a key in a list
     * Delete an index in a map
     * Delete a key in a map that doesn't exist
     * Delete an index in a list that doesn't exist
     *
     */
    @Test
    public void testNegativeDelete() throws IOException,
            JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException {
        // Set up test document
        String jsonString = DebugUtils.readJSONFromAFile("src/main/resources/jsonTestMaterial.json");
        Map jsonAsMap = Utils.convertJSONFromStringToMap(jsonString);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, jsonAsMap);

        // Test different delete paths
        String jsonPath;
        Object originalObject;
        Object deletedObjectRead;

        // Delete a key in an existing map where key does not exist
        jsonPath = "$.example1.nokey";
        originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the original object doesn't exist
        Assert.assertNull(originalObject);
        documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
        // This call will succeed
        deletedObjectRead = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
        // Check the deleted object does not exist
        Assert.assertNull(deletedObjectRead);

        // Delete an out of range element in an existing list
        jsonPath = "$.example2[3]";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Delete a key in a list
        jsonPath = "$.example2.nokey";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Delete an index in a map
        jsonPath = "$.example1[1]";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.NotAListException ignored) {}

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.KeyNotFoundException ignored) {}

        // Delete a key in a map that doesn't exist
        jsonPath = "$.nokey.nokey";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Delete an index in a list that doesn't exist
        jsonPath = "$.nolist[1]";
        // accessing this path throws an error so we know it's not there to start with
        try {
            originalObject = documentClient.get(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}

        // Delete call should throw an error
        try {
            documentClient.delete(TEST_AEROSPIKE_KEY, jsonPath);
            Assert.fail("Should have thrown an error - " + jsonPath + " doesn't exist");
        } catch (AerospikeDocumentClient.ObjectNotFoundException ignored) {}
    }
}
