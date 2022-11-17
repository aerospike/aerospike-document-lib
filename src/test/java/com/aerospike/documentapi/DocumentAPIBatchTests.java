package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.documentapi.batch.AppendBatchOperation;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.batch.DeleteBatchOperation;
import com.aerospike.documentapi.batch.GetBatchOperation;
import com.aerospike.documentapi.batch.PutBatchOperation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import lombok.Getter;
import lombok.Setter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.GET;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.APPEND;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.DELETE;
import static com.aerospike.documentapi.DocumentAPIBatchTests.BatchOperationEnum.PUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DocumentAPIBatchTests extends BaseTestConfig {

    @Test
    public void testPositiveBatchGet() throws IOException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        Map<String, Object> jsonNodeAsMap = JsonConverters.convertJsonNodeToMap(jsonNode);

        Map<String, Object> jsonPathsMap = new LinkedHashMap<>();
        jsonPathsMap.put("$", jsonNodeAsMap);
        jsonPathsMap.put("$.example1", jsonNodeAsMap.get("example1"));
        jsonPathsMap.put("$.example3[1]", ((List<?>) jsonNodeAsMap.get("example3")).get(1));
        jsonPathsMap.put("$.example4.key10", ((Map<?, ?>) jsonNodeAsMap.get("example4")).get("key10"));
        Iterator<String> iterator = jsonPathsMap.keySet().iterator();

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList = new ArrayList<>();

        // adding similar document bins with different jsonPath strings
        IntStream.range(0, jsonPathsMap.size()).forEachOrdered(i -> {
            Key key = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY + i);
            String binName = documentBinName + i;
            documentClient.put(key, binName, jsonNode);

            BatchOperation batchOp = new GetBatchOperation(
                    key,
                    Collections.singletonList(binName),
                    iterator.next()
            );
            batchOpsList.add(batchOp);
        });

        batchRecords = documentClient.batchPerform(batchOpsList);

        int i = 0;
        for (Map.Entry<String, Object> entry : jsonPathsMap.entrySet()) {
            String binName = batchOpsList.get(i).getBinNames().iterator().next();
            assertTrue(TestJsonConverters.jsonEquals(batchRecords.get(i).record.getValue(binName), entry.getValue()));
            i++;
        }
    }

    @Test
    public void testNegativeBatchGet() throws IOException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // non-existing key
        inputsList.add(new BatchOperationInput("$.example3[1].key08.nonexistentkey", GET));
        // referencing a map as if it was a list
        inputsList.add(new BatchOperationInput("$.example1[1]", GET));
        // referencing a primitive as if it was a map
        inputsList.add(new BatchOperationInput("$.example4.key10.key11.nonexistentkey", GET));
        // referencing a primitive as if it was a list
        inputsList.add(new BatchOperationInput("$.example4.key10.key11[2]", GET));
        // referencing a non-existing list item
        inputsList.add(new BatchOperationInput("$.example4.key13.key15[9]", GET));
        // referencing a non-existing map
        inputsList.add(new BatchOperationInput("$.example4.nosuchkey.nosuchkey", GET));
        // referencing a non-existing list
        inputsList.add(new BatchOperationInput("$.example4.nosuchkey[1]", GET));

        List<BatchOperation> batchOpsList;

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                null,
                null,
                false
        );

        documentClient.batchPerform(batchOpsList);

        // making sure all records contain the resulting record == null and the necessary resulting code
        // PARAMETER_ERROR = 4, BIN_TYPE_ERROR = 12, OP_NOT_APPLICABLE = 26
        Integer[] errorCodes = {4, 12, 26};
        batchOpsList.forEach(batchOp -> assertTrue(batchOp.getBatchRecord().record == null
                && (Arrays.asList(errorCodes).contains(batchOp.getBatchRecord().resultCode))));
    }

    @Test
    public void testPositiveBatchPut() throws IOException, DocumentApiException, JsonPathParser.JsonParseException {
        // Set up the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // putting a new key into an existing map
        inputsList.add(new BatchOperationInput("$.example1.key27", PUT));
        // putting a new value into an existing array
        inputsList.add(new BatchOperationInput("$.example1.key01[10]", PUT));

        List<BatchOperation> batchOpsList;
        int putValue = 70;

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                putValue,
                null,
                false
        );

        documentClient.batchPerform(batchOpsList);

        // Check the value put previously
        for (BatchOperation batchOp : batchOpsList) {
            Object objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            // Check that the last element in the list we appended to is the value we added
            assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, putValue));
        }
    }

    @Test
    public void testNegativeBatchPut() throws IOException, DocumentApiException {
        // Set up the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // putting a key into a map that doesn't exist
        inputsList.add(new BatchOperationInput("$.example9.key01", PUT));
        // putting to a list that doesn't exist
        inputsList.add(new BatchOperationInput("$.example9[2]", PUT));
        // treating a map as if it was a list
        inputsList.add(new BatchOperationInput("$.example2.key09", PUT));
        // treating a list as if it was a map
        inputsList.add(new BatchOperationInput("$.example1[1]", PUT));

        List<BatchOperation> batchOpsList;
        int putValue = 70;

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                putValue,
                null,
                false
        );

        documentClient.batchPerform(batchOpsList);

        // making sure all records contain the resulting record == null and the necessary resulting code
        // OP_NOT_APPLICABLE = 26, PARAMETER_ERROR = 4
        batchOpsList.forEach(batchOp -> assertTrue(batchOp.getBatchRecord().record == null
                && (batchOp.getBatchRecord().resultCode == 26 || batchOp.getBatchRecord().resultCode == 4)));
    }

    @Test
    public void testPositiveBatchAppend() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // appending to an array referenced by a key
        inputsList.add(new BatchOperationInput("$.example1.key01", APPEND));
        // appending to an array referenced by an index
        inputsList.add(new BatchOperationInput("$.example4.key19[3]", APPEND));

        List<BatchOperation> batchOpsList;
        int appendValue = 82;

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                null,
                appendValue,
                false
        );

        documentClient.batchPerform(batchOpsList);

        // Check that the last element in the list we appended to is the value we added
        for (BatchOperation batchOp : batchOpsList) {
            List<?> appendedList = (List<?>) documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            // Check that the last element in the list we appended to is the value we added
            assertTrue(appendedList != null && TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), appendValue));
        }
    }

    @Test
    public void testNegativeBatchAppend() throws IOException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // appending to a list that doesn't exist
        inputsList.add(new BatchOperationInput("$.example99", APPEND));
        // appending to a key that doesn't exist
        inputsList.add(new BatchOperationInput("$.example3.key09", APPEND));
        // appending to a map
        inputsList.add(new BatchOperationInput("$.example1", APPEND));
        // appending to a primitive
        inputsList.add(new BatchOperationInput("$.example2.key02", APPEND));

        List<BatchOperation> batchOpsList;
        int appendValue = 82;

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                null,
                appendValue,
                false
        );

        documentClient.batchPerform(batchOpsList);

        // making sure all records contain the resulting record == null and the necessary resulting code
        // PARAMETER_ERROR = 4, BIN_TYPE_ERROR = 12, OP_NOT_APPLICABLE = 26
        Integer[] errorCodes = {4, 12, 26};
        batchOpsList.forEach(batchOp -> assertTrue(batchOp.getBatchRecord().record == null
                && (Arrays.asList(errorCodes).contains(batchOp.getBatchRecord().resultCode))));
    }

    @Test
    public void testPositiveBatchDelete() throws IOException,
            JsonPathParser.JsonParseException, DocumentApiException {
        // Set up test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

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

        List<BatchOperation> batchOpsList = new ArrayList<>();
        Object[] originalObjects = new Object[6];

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                null,
                null,
                false
        );

        // reading original objects
        for (BatchOperation batchOp : batchOpsList) {
            Object originalObject = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
            originalObjects[batchOpsList.indexOf(batchOp)] = originalObject;

            // Check the original object exists
            assertTrue(originalObject instanceof String || originalObject instanceof Map || originalObject instanceof List);
        }

        documentClient.batchPerform(batchOpsList);

        // checking the deleted objects
        for (BatchOperation batchOp : batchOpsList) {
            // Check the deleted object does not exist
            assertTrue(jsonPathDoesNotExist(
                    documentClient,
                    batchOp.getKey(),
                    batchOp.getBinNames().iterator().next(),
                    batchOp.getJsonPath(),
                    originalObjects[batchOpsList.indexOf(batchOp)])
            );
        }
    }

    @Test
    public void testNegativeBatchDelete() throws IOException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

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

        List<BatchOperation> batchOpsList;

        // adding similar document bins with different jsonPath strings
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                null,
                null,
                false
        );

        documentClient.batchPerform(batchOpsList);

        Integer[] errorCodes = {4, 12, 26};
        for (BatchOperation batchOp : batchOpsList) {
            // in case of deleting a non-existing key of the existing map
            // the response has a non-null record with resultCode == 0
            // and record.bins containing null value
            if (batchOp.getJsonPath().equals("$.example1.nokey")) {
                assertTrue(
                        batchOp.getBatchRecord().record != null
                                && batchOp.getBatchRecord().resultCode == 0
                                && batchOp.getBatchRecord().record.bins.get(batchOp.getBinNames().iterator().next()) == null
                );
            } else {
                // making sure all records contain the resulting record == null and the necessary resulting code
                // PARAMETER_ERROR = 4, BIN_TYPE_ERROR = 12, OP_NOT_APPLICABLE = 26
                assertTrue(batchOp.getBatchRecord().record == null
                        && (Arrays.asList(errorCodes).contains(batchOp.getBatchRecord().resultCode)));
            }
        }
    }

    @Test
    public void testPositiveBatchDeleteRootElement() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        documentClient.put(TEST_AEROSPIKE_KEY, documentBinName, jsonNode);

        String jsonPath = "$";
        BatchOperation batchOp = new DeleteBatchOperation(
                TEST_AEROSPIKE_KEY,
                Collections.singletonList(documentBinName),
                jsonPath
        );
        documentClient.batchPerform(Collections.singletonList(batchOp));

        Object objectFromDB = documentClient.get(TEST_AEROSPIKE_KEY, documentBinName, jsonPath);
        assertTrue(((Map<?, ?>) objectFromDB).isEmpty());
    }

    @Test
    public void testPositiveBatchMix() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // reading the whole json
        inputsList.add(new BatchOperationInput("$", GET));
        // putting a new key into an existing map
        inputsList.add(new BatchOperationInput("$.example1.key27", PUT));
        // appending to an array referenced by an index
        inputsList.add(new BatchOperationInput("$.example1.key01", APPEND));
        // deleting a map entry using a map reference
        inputsList.add(new BatchOperationInput("$.example1.key01", DELETE));

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList;

        int objToPut = 86;
        int objToAppend = 87;

        final Object[] originalObject = new Object[1];

        // adding similar document bins with different jsonPath strings and different operations
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        batchRecords = documentClient.batchPerform(batchOpsList);

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
                    Object objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                    // Check that the last element in the list we appended to is the value we added
                    assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, objToPut));
                    break;
                case 2:
                    List<?> appendedList = (List<?>) documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                    // Check that the last element in the list we appended to is the value we added
                    assertTrue(appendedList != null && TestJsonConverters.jsonEquals(appendedList.get(appendedList.size() - 1), objToAppend));
                    break;
                case 3:
                    assertTrue(jsonPathDoesNotExist(
                            documentClient, batchOp.getKey(),
                            binName,
                            batchOp.getJsonPath(),
                            originalObject)
                    );
                    break;
            }

            i++;
        }
    }

    @Test
    public void testPositiveBatchMix2StepWildcard() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

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

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList = new ArrayList<>();

        // adding similar document bins with different jsonPath strings and different operations
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        batchRecords = documentClient.batchPerform(batchOpsList);

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
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                    modifiedJson = JsonPath.parse(testMaterialJson).set(inputsList.get(i).getJsonPath(), objToPut).json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
                case 4:
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                    modifiedJson = JsonPath.parse(testMaterialJson).add(inputsList.get(i).getJsonPath(), objToAppend).json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
                case 5:
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                    modifiedJson = JsonPath.parse(testMaterialJson).delete(inputsList.get(i).getJsonPath()).json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
            }

            i++;
        }
    }

    @Test
    public void testPositiveBatchMix2StepWildcardMultipleBins() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // reading example2, 1 step
        inputsList.add(new BatchOperationInput("$.example2.*", GET));
        // reading by the keys with values == "", 1 step with post-production
        inputsList.add(new BatchOperationInput("$.example2[*].key06", GET));
        // reading a map, 1 step
        inputsList.add(new BatchOperationInput("$.example1", GET));
        // putting a value to the existing "key03" in all elements, 2 steps
//        inputsList.add(new BatchOperationInput("$.example2[*].key03", PUT));
        // reading previously set "key03", 1 step
        inputsList.add(new BatchOperationInput("$.example2[*].key03", GET));
        // appending a value to the end of "key03" array for every element, 1 step

        List<String> objToPut = new ArrayList<>();
        objToPut.add("86");
        String objToAppend = "87";
        String[] binNames = {"documentBin0", "documentBin1", "documentBin2", "documentBin3", "documentBin4"};

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList;

        // adding similar document bins with different jsonPath strings and different operations
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                true,
                binNames
        );

        batchRecords = documentClient.batchPerform(batchOpsList);

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
                objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());

                modifiedJson = context.set(inputsList.get(i).getJsonPath(), objToPut).json();
                expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
            } else if (batchOp.getClass().equals(APPEND.getBatchOperationClass())) {
                objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                modifiedJson = context.add(inputsList.get(i).getJsonPath(), objToAppend).json();
                expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
            } else if (batchOp.getClass().equals(DELETE.getBatchOperationClass())) {
                objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                modifiedJson = context.delete(inputsList.get(i).getJsonPath()).json();
                expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
            }

            i++;
        }
    }

    @Test
    public void testNegativeBatchMix2StepWildcardIncorrectParts() throws IOException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // adding non-existing jsonPaths
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key01", GET));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key02", PUT));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key03", APPEND));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key04", DELETE));

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList;

        String objToPut = "86";
        int objToAppend = 87;

        // adding similar document bins with different jsonPath strings and different operations
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        batchRecords = documentClient.batchPerform(batchOpsList);

        // making sure all records contain the correct negative result code
        batchRecords.forEach(batchRec -> assertEquals(-2, batchRec.resultCode));
    }

    @Test
    public void testNegativeBatchMix2StepWildcardIncorrectKeys() throws IOException, DocumentApiException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        String objToPut = "86";
        int objToAppend = 87;

        List<BatchOperationInput> inputsList = new ArrayList<>();
        // adding non-existing jsonPaths
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", GET));
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", PUT));
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", APPEND));
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", DELETE));

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList = new ArrayList<>();

        // adding similar document bins with different jsonPath strings and different operations
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        batchRecords = documentClient.batchPerform(batchOpsList);

        // making sure all records contain the correct negative result code
        batchRecords.forEach(batchRec -> assertEquals(-2, batchRec.resultCode));
    }

    @Test
    public void testBatchMix2StepWildcardNegativeAndPositive() throws IOException, DocumentApiException, JsonPathParser.JsonParseException {
        // Load the test document
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(testMaterialJson);
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        // adding non-existing jsonPaths
        List<BatchOperationInput> inputsList = new ArrayList<>();
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].keyNonE", GET));
        inputsList.add(new BatchOperationInput("$.example2[*].key03", PUT)); // correct path
        inputsList.add(new BatchOperationInput("$.example4[*].keyNonE", APPEND));
        inputsList.add(new BatchOperationInput("$.exampleNonE[*].key50", DELETE));

        String objToPut = "86";
        int objToAppend = 87;

        List<BatchRecord> batchRecords;
        List<BatchOperation> batchOpsList = new ArrayList<>();

        // adding similar document bins with different jsonPath strings and different operations
        batchOpsList = createBatchOperations(
                documentClient,
                jsonNode,
                inputsList,
                objToPut,
                objToAppend,
                false
        );

        batchRecords = documentClient.batchPerform(batchOpsList);

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
                    objFromDb = documentClient.get(batchOp.getKey(), batchOp.getBinNames().iterator().next(), batchOp.getJsonPath());
                    modifiedJson = JsonPath.parse(testMaterialJson).set(inputsList.get(i).getJsonPath(), objToPut).json();
                    expectedObject = JsonPath.read(modifiedJson, inputsList.get(i).getJsonPath());

                    assertTrue(objFromDb != null && TestJsonConverters.jsonEquals(objFromDb, expectedObject));
                    break;
            }

            i++;
        }
    }

    private List<BatchOperation> createBatchOperations(
            AerospikeDocumentClient documentClient,
            JsonNode jsonNode,
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
                singleBinName = documentBinName + i;
                documentClient.put(key, singleBinName, jsonNode);
            } else {
                if (!sameKeys || i == 0) { //  creating only once in case of the same keys and multiple bins
                    binNamesList.forEach(bName -> documentClient.put(key, bName, jsonNode));
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
            BatchOperationInput batchOperationInput)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException {
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
            AerospikeDocumentClient documentClient,
            Key key,
            String binName,
            String jsonPath,
            Object originalObject
    ) throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        Object res;

        try {
            res = documentClient.get(key, binName, jsonPath);
        } catch (DocumentApiException.ObjectNotFoundException ignored) {
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
