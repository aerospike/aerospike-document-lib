package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.documentapi.batch.AppendBatchOperation;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.batch.DeleteBatchOperation;
import com.aerospike.documentapi.batch.GetBatchOperation;
import com.aerospike.documentapi.batch.PutBatchOperation;
import com.aerospike.documentapi.util.JsonConverters;
import com.aerospike.documentapi.util.TestJsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Getter;
import lombok.Setter;
import net.minidev.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.client.ResultCode.BIN_TYPE_ERROR;
import static com.aerospike.client.ResultCode.OP_NOT_APPLICABLE;
import static com.aerospike.client.ResultCode.PARAMETER_ERROR;
import static com.aerospike.client.ResultCode.PARSE_ERROR;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.APPEND;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.DELETE;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.GET;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.PUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DocumentAPIBatchTests extends BaseTestConfig {

    /**
     * Check the correct document content retrieval in a batch of single step operations.
     * <ul>
     * <li>The whole document.</li>
     * <li>First level element.</li>
     * <li>An array element.</li>
     * <li>A map element.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchGet() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);

        Map<String, Object> jsonPathsMap = new LinkedHashMap<>();
        jsonPathsMap.put("$", jsonNodeAsMap);
        jsonPathsMap.put("$.example1", jsonNodeAsMap.get("example1"));
        jsonPathsMap.put("$.example3[1]", ((List<?>) jsonNodeAsMap.get("example3")).get(1));
        jsonPathsMap.put("$.example4.key10", ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10"));
        Iterator<String> iterator = jsonPathsMap.keySet().iterator();

        List<BatchOperation> batchOpsList = new ArrayList<>();

        // adding similar document bins with different jsonPath strings
        IntStream.range(0, jsonPathsMap.size()).forEachOrdered(i -> {
            Key key = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY + i);
            String binName = DOCUMENT_BIN_NAME + i;
            documentClient.put(key, binName, jsonNode);

            BatchOperation batchOp = new GetBatchOperation(
                    key,
                    Collections.singletonList(binName),
                    iterator.next()
            );
            batchOpsList.add(batchOp);
        });

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, batchRecords.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        int i = 0;
        for (Map.Entry<String, Object> entry : jsonPathsMap.entrySet()) {
            String binName = batchOpsList.get(i).getBinNames().iterator().next();
            assertTrue(TestJsonConverters.jsonEquals(batchRecords.get(i).record.getValue(binName), entry.getValue()));
            i++;
        }
    }

    /**
     * Check the correct response of retrieving document content in a batch of single step operations using incorrect
     * path.
     * <ul>
     * <li>Non-existing key.</li>
     * <li>Referencing a map as if it were a list.</li>
     * <li>Referencing a primitive as if it were a map.</li>
     * <li>Referencing a primitive as if it were a list.</li>
     * <li>Referencing a non-existing list item.</li>
     * <li>Referencing a non-existing map.</li>
     * <li>Referencing a non-existing list.</li>
     * </ul>
     */
    @Test
    void testNegativeBatchGet() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // non-existing key
        inputsList.add(new BatchOperationInput("$.example3[1].key08.nonexistentkey", GET));
        // referencing a map as if it were a list
        inputsList.add(new BatchOperationInput("$.example1[1]", GET));
        // referencing a primitive as if it were a map
        inputsList.add(new BatchOperationInput("$.example4.key10.key11.nonexistentkey", GET));
        // referencing a primitive as if it were a list
        inputsList.add(new BatchOperationInput("$.example4.key10.key11[2]", GET));
        // referencing a non-existing list item
        inputsList.add(new BatchOperationInput("$.example4.key13.key15[9]", GET));
        // referencing a non-existing map
        inputsList.add(new BatchOperationInput("$.example4.nosuchkey.nosuchkey", GET));
        // referencing a non-existing list
        inputsList.add(new BatchOperationInput("$.example4.nosuchkey[1]", GET));

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                null,
                null,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertTrue(results.stream().anyMatch(res -> res.resultCode != ResultCode.OK));

        // making sure all records contain the resulting record == null and the necessary resulting code
        Integer[] errorCodes = {PARAMETER_ERROR, BIN_TYPE_ERROR, OP_NOT_APPLICABLE};
        batchOpsList.forEach(batchOp -> assertTrue(batchOp.getBatchRecord().record == null
                && (Arrays.asList(errorCodes).contains(batchOp.getBatchRecord().resultCode))));
    }

    /**
     * Check the correct document content retrieval in a batch of single step operations with the same keys.
     * <ul>
     * <li>The whole document.</li>
     * <li>First level element.</li>
     * <li>An array element.</li>
     * <li>A map element.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchGetSameKeys() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);

        Map<String, Object> jsonPathsMap = new LinkedHashMap<>();
        jsonPathsMap.put("$", jsonNodeAsMap);
        jsonPathsMap.put("$.example1", jsonNodeAsMap.get("example1"));
        jsonPathsMap.put("$.example3[1]", ((List<?>) jsonNodeAsMap.get("example3")).get(1));
        jsonPathsMap.put("$.example4.key10", ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10"));
        Iterator<String> iterator = jsonPathsMap.keySet().iterator();

        List<BatchOperation> batchOpsList = new ArrayList<>();

        // the same key and the same document bin, different jsonPath strings
        IntStream.range(0, jsonPathsMap.size()).forEachOrdered(i -> {
            Key key = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY + "1111");
            String binName = DOCUMENT_BIN_NAME + "22";
            documentClient.put(key, binName, jsonNode);

            BatchOperation batchOp = new GetBatchOperation(
                    key,
                    Collections.singletonList(binName),
                    iterator.next()
            );
            batchOpsList.add(batchOp);
        });

        // read operations with the same keys referring to the same bins can run in a batch
        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, batchRecords.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        int i = 0;
        for (Map.Entry<String, Object> entry : jsonPathsMap.entrySet()) {
            String binName = batchOpsList.get(i).getBinNames().iterator().next();
            assertTrue(TestJsonConverters.jsonEquals(batchRecords.get(i).record.getValue(binName), entry.getValue()));
            i++;
        }
    }

    /**
     * Check a batch of single step PUT operations.
     * <ul>
     * <li>Putting a new key into an existing map.</li>
     * <li>Putting a new value into an existing array.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchPut() {
        // Set up the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // putting a new key into an existing map
        inputsList.add(new BatchOperationInput("$.example1.key27", PUT));
        // putting a new value into an existing array
        inputsList.add(new BatchOperationInput("$.example1.key01[10]", PUT));

        int putValue = 70;

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                putValue,
                null,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, results.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        // Check the value put previously
        for (BatchOperation batchOp : batchOpsList) {
            Object objFromDb = documentClient.get(batchOp.getKey(),
                    batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            // Check that the last element in the list we put to is the initial put value
            assertNotNull(objFromDb);
            assertTrue(TestJsonConverters.jsonEquals(objFromDb, putValue));
        }
    }

    /**
     * Check a batch of 2-step step PUT operations.
     * <ul>
     * <li>Putting to the existing position.</li>
     * <li>Putting to a new position.</li>
     * <li>Putting to a new position in a more complex structure.</li>
     * <li>Putting to a new key.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchPutWildcard() {
        // Set up the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // putting to the existing position
        inputsList.add(new BatchOperationInput("$.example2[*].key01[3]", PUT));
        // putting to a new position
        inputsList.add(new BatchOperationInput("$.example2[*].key01[4]", PUT));
        // putting to a new position in a more complex structure
        inputsList.add(new BatchOperationInput("$.example2[*].key07[*].key10[4]", PUT));
        // putting to a new key
        inputsList.add(new BatchOperationInput("$.example3[*].key76", PUT));

        int putValue = 70;

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                putValue,
                null,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, results.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        // Check the value put previously
        for (BatchOperation batchOp : batchOpsList) {
            Object objFromDb = documentClient.get(batchOp.getKey(),
                    batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            if (objFromDb instanceof JSONArray) {
                for (Object res : (JSONArray) objFromDb) {
                    assertNotNull(res);
                    assertTrue(TestJsonConverters.jsonEquals(res, putValue));
                }
            } else {
                assertNotNull(objFromDb);
                assertTrue(TestJsonConverters.jsonEquals(objFromDb, putValue));
            }
        }
    }

    /**
     * Check the response to a batch of single step PUT operations with incorrect path.
     * <ul>
     * <li>Putting a key into a map that doesn't exist.</li>
     * <li>Putting to a list that doesn't exist.</li>
     * <li>Treating a map as if it were a list.</li>
     * <li>Treating a list as if it were a map.</li>
     * </ul>
     */
    @Test
    void testNegativeBatchPut() {
        // Set up the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // putting a key into a map that doesn't exist
        inputsList.add(new BatchOperationInput("$.example9.key01", PUT));
        // putting to a list that doesn't exist
        inputsList.add(new BatchOperationInput("$.example9[2]", PUT));
        // treating a map as if it were a list
        inputsList.add(new BatchOperationInput("$.example2.key09", PUT));
        // treating a list as if it were a map
        inputsList.add(new BatchOperationInput("$.example1[1]", PUT));

        int putValue = 70;

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                putValue,
                null,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertTrue(results.stream().anyMatch(res -> res.resultCode != ResultCode.OK));

        // making sure all records contain the resulting record == null and the necessary resulting code
        // OP_NOT_APPLICABLE = 26, PARAMETER_ERROR = 4, BIN_TYPE_ERROR = 12
        batchOpsList.forEach(batchOp -> {
            BatchRecord batchRecord = batchOp.getBatchRecord();
            assertTrue(batchRecord.record == null
                    && (batchRecord.resultCode == 26 || batchRecord.resultCode == 4 || batchRecord.resultCode == 12));
        });
    }

    /**
     * Check a batch of single step APPEND operations.
     * <ul>
     * <li>Appending to an array referenced by a key.</li>
     * <li>Appending to an array referenced by an index.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchAppend() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // appending to an array referenced by a key
        inputsList.add(new BatchOperationInput("$.example1.key01", APPEND));
        // appending to an array referenced by an index
        inputsList.add(new BatchOperationInput("$.example4.key19[3]", APPEND));

        int appendValue = 82;

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                null,
                appendValue,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, results.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        for (BatchOperation batchOp : batchOpsList) {
            List<?> appendedList = (List<?>) documentClient.get(batchOp.getKey(),
                    batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            // Check that the last element in the list we appended to is the value we added
            assertNotNull(appendedList);
            assertTrue(TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), appendValue));
        }
    }

    /**
     * Check response to a batch of single step APPEND operations using incorrect path.
     * <ul>
     * <li>Appending to a list that doesn't exist.</li>
     * <li>Appending to a key that doesn't exist.</li>
     * <li>Appending to a map.</li>
     * <li>Appending to a primitive.</li>
     * </ul>
     */
    @Test
    void testNegativeBatchAppend() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // appending to a list that doesn't exist
        inputsList.add(new BatchOperationInput("$.example99", APPEND));
        // appending to a key that doesn't exist
        inputsList.add(new BatchOperationInput("$.example3.key09", APPEND));
        // appending to a map
        inputsList.add(new BatchOperationInput("$.example1", APPEND));
        // appending to a primitive
        inputsList.add(new BatchOperationInput("$.example2.key02", APPEND));

        int appendValue = 82;

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                null,
                appendValue,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertTrue(results.stream().anyMatch(res -> res.resultCode != ResultCode.OK));

        // making sure all records contain the resulting record == null and the necessary resulting code
        Integer[] errorCodes = {PARAMETER_ERROR, BIN_TYPE_ERROR, OP_NOT_APPLICABLE};
        batchOpsList.forEach(batchOp -> assertTrue(batchOp.getBatchRecord().record == null
                && (Arrays.asList(errorCodes).contains(batchOp.getBatchRecord().resultCode))));
    }

    /**
     * Check a batch of single step DELETE operations.
     * <ul>
     * <li>Deleting a primitive using a map reference.</li>
     * <li>Deleting a primitive using a list reference.</li>
     * <li>Deleting a map using a map reference.</li>
     * <li>Deleting a list using a map reference.</li>
     * <li>Deleting a map using a list reference.</li>
     * <li>Deleting a list using a list reference.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchDelete() {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // deleting a primitive using a map reference
        inputsList.add(new BatchOperationInput("$.example4.key10.key12", DELETE));
        // deleting a primitive using a list reference
        inputsList.add(new BatchOperationInput("$.example3[1].key08[2]", DELETE));
        // deleting a map using a map reference
        inputsList.add(new BatchOperationInput("$.example4.key13", DELETE));
        // deleting a list using a map reference
        inputsList.add(new BatchOperationInput("$.example4.key19[2].key20", DELETE));
        // deleting a map using a list reference
        inputsList.add(new BatchOperationInput("$.example2[0]", DELETE));
        // deleting a list using a list reference
        inputsList.add(new BatchOperationInput("$.example4.key19[4]", DELETE));

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                null,
                null,
                false
        );

        Object[] originalObjects = new Object[6];

        // reading original objects
        for (BatchOperation batchOp : batchOpsList) {
            Object originalObject = documentClient.get(batchOp.getKey(),
                    batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            originalObjects[batchOpsList.indexOf(batchOp)] = originalObject;

            // Check the original object exists
            assertTrue(originalObject instanceof String || originalObject instanceof Map
                    || originalObject instanceof List);
        }

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, results.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        // checking the deleted objects
        for (BatchOperation batchOp : batchOpsList) {
            // Check the deleted object does not exist
            assertTrue(jsonPathDoesNotExist(
                    batchOp.getKey(),
                    batchOp.getBinNames().iterator().next(),
                    batchOp.getJsonPath(),
                    originalObjects[batchOpsList.indexOf(batchOp)])
            );
        }
    }

    /**
     * Check response to a batch of single step DELETE operations using incorrect path.
     * <ul>
     * <li>Deleting a non-existing key in an existing map.</li>
     * <li>Deleting an out of range element in an existing list.</li>
     * <li>Deleting a key in a list.</li>
     * <li>Deleting an index in a map.</li>
     * <li>Deleting a key in a non-existing map.</li>
     * <li>Deleting an index in a non-existing list.</li>
     * </ul>
     */
    @Test
    void testNegativeBatchDelete() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // deleting a non-existing key in an existing map
        inputsList.add(new BatchOperationInput("$.example1.nokey", DELETE));
        // deleting an out of range element in an existing list
        inputsList.add(new BatchOperationInput("$.example2[3]", DELETE));
        // deleting a key in a list
        inputsList.add(new BatchOperationInput("$.example2.nokey", DELETE));
        // deleting an index in a map
        inputsList.add(new BatchOperationInput("$.example1[1]", DELETE));
        // deleting a key in a non-existing map
        inputsList.add(new BatchOperationInput("$.nokey.nokey", DELETE));
        // deleting an index in a non-existing list
        inputsList.add(new BatchOperationInput("$.nolist[1]", DELETE));

        // adding similar document bins with different jsonPath strings
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                null,
                null,
                false
        );

        List<BatchRecord> results = documentClient.batchPerform(batchOpsList, true);
        assertTrue(results.stream().anyMatch(res -> res.resultCode != ResultCode.OK));

        Integer[] errorCodes = {PARAMETER_ERROR, BIN_TYPE_ERROR, OP_NOT_APPLICABLE};
        for (BatchOperation batchOp : batchOpsList) {
            // in case of deleting a non-existing key of the existing map
            // the response has a non-null record with resultCode == 0
            // and record.bins containing null value
            if (batchOp.getJsonPath().equals("$.example1.nokey")) {
                assertNotNull(batchOp.getBatchRecord().record);
                assertEquals(0, batchOp.getBatchRecord().resultCode);
                assertNull(batchOp.getBatchRecord().record.bins.get(batchOp.getBinNames().iterator().next()));
            } else {
                // making sure all records contain the resulting record == null and the necessary resulting code
                assertNull(batchOp.getBatchRecord().record);
                assertTrue(Arrays.asList(errorCodes).contains(batchOp.getBatchRecord().resultCode));
            }
        }
    }

    @Test
    void testPositiveBatchDeleteRootElement() {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);

        String jsonPath = "$";
        BatchOperation batchOp = new DeleteBatchOperation(
                TEST_AEROSPIKE_KEY,
                Collections.singletonList(DOCUMENT_BIN_NAME),
                jsonPath
        );
        List<BatchRecord> results = documentClient.batchPerform(Collections.singletonList(batchOp), true);
        assertEquals(0, results.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonPath);
        assertTrue(((Map<?, ?>) objectFromDB).isEmpty());
    }

    /**
     * Check a batch of different types single step operations using different keys.
     * <ul>
     * <li>Reading the whole json.</li>
     * <li>Putting a new key into an existing map.</li>
     * <li>Appending to an array referenced by an index.</li>
     * <li>Deleting a map entry using a map reference.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchMix() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // reading the whole json
        inputsList.add(new BatchOperationInput("$", GET));
        // putting a new key into an existing map
        inputsList.add(new BatchOperationInput("$.example1.key27", PUT));
        // appending to an array referenced by an index
        inputsList.add(new BatchOperationInput("$.example1.key01", APPEND));
        // deleting a map entry using a map reference
        inputsList.add(new BatchOperationInput("$.example1.key01", DELETE));

        int objToPut = 86;
        int objToAppend = 87;

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, batchRecords.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        final Object[] originalObject = new Object[1];

        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);
        int i = 0;
        for (BatchRecord batchRecord : batchRecords) {
            BatchOperation batchOp = batchOpsList.get(i);
            String binName = batchOp.getBinNames().iterator().next();

            switch (i) {
                case 0:
                    assertTrue(TestJsonConverters.jsonEquals(batchRecord.record.getValue(binName), jsonNodeAsMap));
                    break;
                case 1:
                    Object objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                            batchOp.getJsonPath());
                    // Check that the last element in the list we put to is the initial put value
                    assertNotNull(objFromDb);
                    assertTrue(TestJsonConverters.jsonEquals(objFromDb, objToPut));
                    break;
                case 2:
                    List<?> appendedList = (List<?>) documentClient.get(batchOp.getKey(),
                            batchOp.getBinNames().iterator().next(), batchOp.getJsonPath()
                    );
                    // Check that the last element in the list we appended to is the value we added
                    assertNotNull(appendedList);
                    assertTrue(TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), objToAppend));
                    break;
                case 3:
                    assertTrue(jsonPathDoesNotExist(
                            batchOp.getKey(),
                            binName,
                            batchOp.getJsonPath(),
                            originalObject)
                    );
                    break;
            }
            i++;
        }
    }

    /**
     * Check a batch of different types single step operations (each with a different bin) using the same keys.
     * <ul>
     * <li>Reading the whole json.</li>
     * <li>Putting a new key into an existing map.</li>
     * <li>Appending to an array referenced by an index.</li>
     * <li>Deleting a map entry using a map reference.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchMixSameKeys() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // reading the whole json
        inputsList.add(new BatchOperationInput("$", GET));
        // putting a new key into an existing map
        inputsList.add(new BatchOperationInput("$.example1.key27", PUT));
        // appending to an array referenced by an index
        inputsList.add(new BatchOperationInput("$.example1.key01", APPEND));
        // deleting a map entry using a map reference
        inputsList.add(new BatchOperationInput("$.example1.key01", DELETE));

        int objToPut = 86;
        int objToAppend = 87;

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                true
        );

        // operations order is not guaranteed
        // in this example each operation uses a different bin, so no overlapping / conflicts
        // otherwise (referring to the same bins) write-type operations with the same keys cannot run in a batch
        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, batchRecords.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        final Object[] originalObject = new Object[1];

        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);
        int i = 0;
        for (BatchRecord batchRecord : batchRecords) {
            BatchOperation batchOp = batchOpsList.get(i);
            String binName = batchOp.getBinNames().iterator().next();

            switch (i) {
                case 0:
                    assertTrue(TestJsonConverters.jsonEquals(batchRecord.record.getValue(binName), jsonNodeAsMap));
                    break;
                case 1:
                    Object objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                            batchOp.getJsonPath());
                    // Check that the last element in the list we put to is the initial put value
                    assertNotNull(objFromDb);
                    assertTrue(TestJsonConverters.jsonEquals(objFromDb, objToPut));
                    break;
                case 2:
                    List<?> appendedList = (List<?>) documentClient.get(batchOp.getKey(),
                            batchOp.getBinNames().iterator().next(), batchOp.getJsonPath()
                    );
                    // Check that the last element in the list we appended to is the value we added
                    assertNotNull(appendedList);
                    assertTrue(TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), objToAppend));
                    break;
                case 3:
                    assertTrue(jsonPathDoesNotExist(
                            batchOp.getKey(),
                            binName,
                            batchOp.getJsonPath(),
                            originalObject)
                    );
                    break;
            }
            i++;
        }
    }

    /**
     * Check a batch of different types 2-step operations using different keys.
     * <ul>
     * <li>Reading the whole document, 1 step.</li>
     * <li>Reading example2, 1 step.</li>
     * <li>Reading by the keys with values == "", 1 step with post-production.</li>
     * <li>Putting a value to "key03" in all elements, 2 steps.</li>
     * <li>Appending a value to the end of "key01" array for every element, 2 steps.</li>
     * <li>Deleting "key05" from all elements, 2 steps.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchMix2StepWildcard() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // reading the whole json, 1 step
        inputsList.add(new BatchOperationInput("$.*", GET));
        // reading the whole example2, 1 step
        inputsList.add(new BatchOperationInput("$.example2.*", GET));
        // reading by the keys with values == "", 1 step with post-production
        inputsList.add(new BatchOperationInput("$.example2[*].key06", GET));
        // putting a value to "key03" in all elements, 2 steps
        inputsList.add(new BatchOperationInput("$.example2[*].key03", PUT));
        // appending a value to the end of "key01" array for every element, 2 steps
        inputsList.add(new BatchOperationInput("$.example2[*].key01", APPEND));
        // deleting "key05" from all elements, 2 steps
        inputsList.add(new BatchOperationInput("$.example2[*].key05", DELETE));

        String objToPut = "86";
        int objToAppend = 87;

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, batchRecords.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        Object objFromDb, modifiedJson, expectedObject;
        int i = 0;
        for (BatchRecord batchRecord : batchRecords) {
            BatchOperation batchOp = batchOpsList.get(i);
            String binName = batchOp.getBinNames().iterator().next();

            switch (i) {
                case 0:
                    // checking the results of "read all" operation
                    expectedObject = JsonConverters.convertJsonNodeToMap(jsonNode);
                    assertTrue(TestJsonConverters.jsonEquals(batchRecord.record.getValue(binName), expectedObject));
                    break;
                case 1:
                case 2:
                    expectedObject = JsonPath.read(testMaterialJson, inputsList.get(i).getJsonPath());
                    assertTrue(TestJsonConverters.jsonEquals(batchRecord.record.getValue(binName), expectedObject));
                    break;
                case 3:
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                            batchOp.getJsonPath()
                    );
                    modifiedJson = JsonPath.parse(testMaterialJson).set(inputsList.get(i).getJsonPath(), objToPut)
                            .json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertNotNull(objFromDb);
                    assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
                case 4:
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                            batchOp.getJsonPath()
                    );
                    modifiedJson = JsonPath.parse(testMaterialJson).add(inputsList.get(i).getJsonPath(), objToAppend)
                            .json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertNotNull(objFromDb);
                    assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
                case 5:
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                            batchOp.getJsonPath()
                    );
                    modifiedJson = JsonPath.parse(testMaterialJson).delete(inputsList.get(i).getJsonPath()).json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertNotNull(objFromDb);
                    assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
            }
            i++;
        }
    }

    /**
     * Check a batch of different types 2-step operations for multiple bins using different keys.
     * <ul>
     * <li>Reading key07 in every element of example2, 1 step with post-production</li>
     * <li>Putting to the existing "key03" in every element of example2, 2 steps.</li>
     * <li>Appending to the end of existing "key01" array for every element of example2, 2 steps.</li>
     * <li>Deleting "key05" for every element of example2, 2 steps.</li>
     * </ul>
     */
    @Test
    void testPositiveBatchMix2StepWildcardMultipleBins() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // reading key07 in every element of example2, 1 step with post-production
        inputsList.add(new BatchOperationInput("$.example2[*].key07", GET));
        // putting a value to the existing "key03", 2 steps
        inputsList.add(new BatchOperationInput("$.example2[0].key03", PUT));
        // appending a value to the end of "key03" array for element 1, 2 steps
        inputsList.add(new BatchOperationInput("$.example2[*].key01", APPEND));
        // deleting "key05" for every element of example2, 2 steps
        inputsList.add(new BatchOperationInput("$.example2[*].key05", DELETE));

        List<String> objToPut = new ArrayList<>();
        objToPut.add("86");
        String objToAppend = "87";
        String[] binNames = {"documentBin0", "documentBin1", "documentBin2", "documentBin3", "documentBin4"};

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false,
                binNames
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);
        assertEquals(0, batchRecords.stream().filter(res -> res.resultCode != ResultCode.OK).count());

        Object objFromDb, modifiedJson, expectedObject;
        DocumentContext context = JsonPath.parse(testMaterialJson);
        int i = 0;
        for (BatchRecord batchRecord : batchRecords) {
            BatchOperation batchOp = batchOpsList.get(i);
            String binName = batchOp.getBinNames().iterator().next();

            // expecting every record to be successful
            assertEquals(0, batchRecord.resultCode);

            if (batchOp.getClass().equals(GET.getBatchOperationClass())) {
                expectedObject = JsonPath.read(testMaterialJson, inputsList.get(i).getJsonPath());
                assertTrue(TestJsonConverters.jsonEquals(batchRecord.record.getValue(binName), expectedObject));
            } else if (batchOp.getClass().equals(PUT.getBatchOperationClass())) {
                objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                        batchOp.getJsonPath());

                modifiedJson = context.set(inputsList.get(i).getJsonPath(), objToPut).json();
                expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                assertNotNull(objFromDb);
                assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
            } else if (batchOp.getClass().equals(APPEND.getBatchOperationClass())) {
                objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                        batchOp.getJsonPath());
                modifiedJson = context.add(inputsList.get(i).getJsonPath(), objToAppend).json();
                expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                assertNotNull(objFromDb);
                assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
            } else if (batchOp.getClass().equals(DELETE.getBatchOperationClass())) {
                objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                        batchOp.getJsonPath());
                modifiedJson = context.delete(inputsList.get(i).getJsonPath()).json();
                expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                assertNotNull(objFromDb);
                assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
            }
            i++;
        }
    }

    /**
     * Check response to a batch of different types 2-step operations using incorrect path.
     * <ul>
     * <li>Non-existing first level element.</li>
     * </ul>
     */
    @Test
    void testNegativeBatchMix2StepWildcardIncorrectParts() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // adding non-existing jsonPaths
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key01", GET));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key02", PUT));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key03", APPEND));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key04", DELETE));

        String objToPut = "86";
        int objToAppend = 87;

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);

        // making sure all records contain the correct negative result code
        batchRecords.forEach(batchRec -> assertEquals(-2, batchRec.resultCode));
    }

    /**
     * Check response to a write-type batch operation for json with integer keys.
     */
    @Test
    void testNegativeBatchWriteJsonIntKeys() {
        Map<Integer, List<Map<Integer, List<String>>>> map = new HashMap<>();
        List<String> innerList = new ArrayList<>();
        innerList.add("A1");
        innerList.add("B1");
        innerList.add("C1");
        innerList.add("D1");
        Map<Integer, List<String>> innerMap = new HashMap<>();
        innerMap.put(3, innerList);
        List<Map<Integer, List<String>>> list = new ArrayList<>();
        list.add(innerMap);
        map.put(2, list);

        // Load the "incorrect json"
        writeDocumentToDB(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, map);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        inputsList.add(new BatchOperationInput("$.2[1].3", PUT)); // resultCode 26
        // JSONPath query analogous to "$.example2[*].key03"
        inputsList.add(new BatchOperationInput("$.2[*].3", PUT)); // resultCode -2

        String objToPut = "86";
        List<BatchOperation> batchOpsList = createBatchOperations(
                map,
                inputsList,
                objToPut,
                null,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);

        // making sure batch record contains the correct result code
        Integer[] errorCodes = {PARSE_ERROR, OP_NOT_APPLICABLE};
        batchRecords.forEach(batchRec -> assertTrue((Arrays.asList(errorCodes).contains(batchRec.resultCode))));
    }

    /**
     * Check response to a write-type batch operation for json with binary list elements.
     */
    @Test
    void testBatchWrite2StepJsonBinaryListValues() {
        Map<String, List<Map<String, Map<String, byte[]>>>> map = new HashMap<>();
        String mapKey = "A1";
        String testMapValue = "This is test1 value ☺";
        Map<String, byte[]> insideMap = new HashMap<>();
        insideMap.put(mapKey, testMapValue.getBytes(StandardCharsets.UTF_8));
        Map<String, Map<String, byte[]>> innerMap = new HashMap<>();
        innerMap.put("3", insideMap);
        List<Map<String, Map<String, byte[]>>> list = new ArrayList<>();
        list.add(innerMap);
        map.put("2", list);

        // Load the "incorrect json"
        writeDocumentToDB(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, map);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // JSONPath query analogous to "$.example2[*].key03"
        inputsList.add(new BatchOperationInput("$.2[*].3", PUT));

        byte[] objToPut = "86".getBytes();
        List<BatchOperation> batchOpsList = createBatchOperations(
                map,
                inputsList,
                objToPut,
                null,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);

        // making sure batch record contains the correct result code
        batchRecords.forEach(batchRec -> assertEquals(0, batchRec.resultCode));
    }

    /**
     * Check response to a batch of different types 2-step operations using incorrect path.
     * <ul>
     * <li>Non-existing key with existing first level element.</li>
     * </ul>
     */
    @Test
    void testNegativeBatchMix2StepWildcardIncorrectKeys() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        String objToPut = "86";
        int objToAppend = 87;

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // adding non-existing jsonPaths
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", GET));
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", PUT));
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", APPEND));
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", DELETE));

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);

        // making sure all records contain the correct negative result code
        batchRecords.forEach(batchRec -> assertEquals(-2, batchRec.resultCode));
    }

    /**
     * Check response to a batch of different types 2-step operations using both correct and incorrect paths.
     * <ul>
     * <li>Reading non-existing key of a non-existing first level element.</li>
     * <li>Putting a value to key03 of every element of example2 using correct path.</li>
     * <li>Appending to a non-existing key of existing first level element.</li>
     * <li>Deleting an existing key of non-existing first level element.</li>
     * </ul>
     */
    @Test
    void testBatchMix2StepWildcardNegativeAndPositive() {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);

        // adding non-existing jsonPaths
        List<BatchOperationInput> inputsList = new ArrayList<>();
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].keyNonE", GET));
        inputsList.add(new BatchOperationInput("$.example2[*].key03", PUT)); // correct path
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", APPEND));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key50", DELETE));

        String objToPut = "86";
        int objToAppend = 87;

        // adding similar document bins with different jsonPath strings and different operations
        List<BatchOperation> batchOpsList = createBatchOperations(
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        List<BatchRecord> batchRecords = documentClient.batchPerform(batchOpsList, true);

        Object objFromDb, modifiedJson, expectedObject;
        int i = 0;
        for (BatchRecord batchRecord : batchRecords) {
            BatchOperation batchOp = batchOpsList.get(i);

            switch (i) {
                case 0:
                case 2:
                case 3:
                    assertEquals(-2, batchRecord.resultCode);
                    break;
                case 1:
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(),
                            batchOp.getJsonPath());
                    modifiedJson = JsonPath.parse(testMaterialJson).set(inputsList.get(i).getJsonPath(), objToPut)
                            .json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertNotNull(objFromDb);
                    assertTrue(TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
            }
            i++;
        }
    }

    private List<BatchOperation> createBatchOperations(
            Object jsonNode,
            List<BatchOperationInput> inputsList,
            Object objToPut,
            Object objToAppend,
            boolean sameKeys,
            String... binNamesToUse
    ) {
        List<String> binNamesList = binNamesToUse.length == 0 ? null
                : Arrays.stream(binNamesToUse)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        List<BatchOperation> batchOpsList = new ArrayList<>();

        Key KEY_CONST = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY);

        IntStream.range(0, inputsList.size()).forEachOrdered(i -> {
            Key key = (sameKeys) ? KEY_CONST : new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY + i);

            String singleBinName = "";
            if (binNamesList == null) {
                singleBinName = DOCUMENT_BIN_NAME + i;
                writeDocumentToDB(key, singleBinName, jsonNode);
            } else {
                if (!sameKeys || i == 0) { //  creating only once in case of the same keys and multiple bins
                    binNamesList.forEach(bName -> writeDocumentToDB(key, bName, jsonNode));
                }
            }

            if (inputsList.get(i).getBatchOpEnum().getBatchOperationClass().equals(PutBatchOperation.class)) {
                inputsList.get(i).setJsonValue(objToPut);
            } else if (inputsList.get(i).getBatchOpEnum().getBatchOperationClass().equals(AppendBatchOperation.class)) {
                inputsList.get(i).setJsonValue(objToAppend);
            }

            BatchOperation batchOp;
            try {
                batchOp = constructTestOperation(
                        key,
                        binNamesList == null ? Collections.singletonList(singleBinName) : binNamesList,
                        inputsList.get(i)
                );
            } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                     IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            batchOpsList.add(batchOp);
        });

        return batchOpsList;
    }

    private BatchOperation constructTestOperation(
            Key key,
            List<String> binNames,
            BatchOperationInput batchOperationInput
    ) throws NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException, ClassNotFoundException {
        BatchOperation batchOp;

        if (batchOperationInput.getJsonValue() != null) {
            batchOp = (BatchOperation) batchOperationInput.getBatchOpEnum()
                    .getBatchOperationClass()
                    .getConstructor(Key.class, Collection.class, String.class, Object.class)
                    .newInstance(key, binNames, batchOperationInput.getJsonPath(), batchOperationInput.getJsonValue());
        } else {
            batchOp = (BatchOperation) batchOperationInput.getBatchOpEnum()
                    .getBatchOperationClass()
                    .getConstructor(Key.class, Collection.class, String.class)
                    .newInstance(key, binNames, batchOperationInput.getJsonPath());
        }

        return batchOp;
    }

    private boolean jsonPathDoesNotExist(
            Key key,
            String binName,
            String jsonPath,
            Object originalObject
    ) {
        Object res;

        try {
            res = documentClient.get(key, binName, jsonPath);
        } catch (DocumentApiException ignored) {
            return true;
        }

        return (res == null || !res.equals(originalObject));
    }

    protected enum BatchOperationEnum {
        GET(GetBatchOperation.class),
        PUT(PutBatchOperation.class),
        APPEND(AppendBatchOperation.class),
        DELETE(DeleteBatchOperation.class);

        @Getter
        private final Class<?> batchOperationClass;

        BatchOperationEnum(Class<?> batchOperationClass) {
            this.batchOperationClass = batchOperationClass;
        }
    }

    @Getter
    private static class BatchOperationInput {

        private final String jsonPath;
        private final BatchOperationEnum batchOpEnum;
        @Setter
        private Object jsonValue = null;

        public BatchOperationInput(String jsonPath, BatchOperationEnum batchOpEnum) {
            this.jsonPath = jsonPath;
            this.batchOpEnum = batchOpEnum;
        }
    }
}
