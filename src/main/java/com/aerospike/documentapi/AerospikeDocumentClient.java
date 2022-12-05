package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.JsonPathQuery;
import com.aerospike.documentapi.policy.DocumentPolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Primary object for accessing and mutating documents.
 */
public class AerospikeDocumentClient implements IAerospikeDocumentClient {

    private final AerospikeDocumentRepository aerospikeDocumentRepository;
    private final Policy readPolicy;
    private final WritePolicy writePolicy;
    private final BatchPolicy batchPolicy;

    public AerospikeDocumentClient(IAerospikeClient client) {
        this.aerospikeDocumentRepository = new AerospikeDocumentRepository(client);
        this.readPolicy = client.getReadPolicyDefault();
        this.writePolicy = client.getWritePolicyDefault();
        this.batchPolicy = client.getBatchPolicyDefault();
    }

    public AerospikeDocumentClient(IAerospikeClient client, DocumentPolicy documentPolicy) {
        this.aerospikeDocumentRepository = new AerospikeDocumentRepository(client);
        this.readPolicy = documentPolicy.getReadPolicy();
        this.writePolicy = documentPolicy.getWritePolicy();
        this.batchPolicy = documentPolicy.getBatchPolicy();
    }

    @Override
    public Object get(Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Object result = aerospikeDocumentRepository.get(readPolicy, documentKey, documentBinName, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            return JsonPathQuery.read(jsonPathObject, result);
        } else {
            return result;
        }
    }

    @Override
    public Object get(Policy customReadPolicy, Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Object result = aerospikeDocumentRepository.get(customReadPolicy, documentKey, documentBinName, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            return JsonPathQuery.read(jsonPathObject, result);
        } else {
            return result;
        }
    }

    @Override
    public Map<String, Object> get(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Map<String, Object> result = aerospikeDocumentRepository.get(readPolicy, documentKey, documentBinNames, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            Map<String, Object> results = new HashMap<>();
            for (String binName : result.keySet()) {
                results.put(binName, JsonPathQuery.read(jsonPathObject, result.get(binName)));
            }
            return results;
        } else {
            return result;
        }
    }

    @Override
    public void put(Key documentKey, String documentBinName, JsonNode jsonNode) {
        aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, jsonNode);
    }

    @Override
    public void put(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Object result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinName, jsonPathObject);
            Object queryResult = JsonPathQuery.putOrSet(jsonPathObject, result, jsonObject);
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, queryResult, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, jsonObject, jsonPathObject);
        }
    }

    @Override
    public void put(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinNames, jsonPathObject);
            Map<String, Object> queryResults = new HashMap<>();
            for (String binName : result.keySet()) {
                queryResults.put(binName, JsonPathQuery.putOrSet(jsonPathObject, result.get(binName), jsonObject));
            }
            aerospikeDocumentRepository.put(writePolicy, documentKey, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinNames, jsonObject, jsonPathObject);
        }
    }

    @Override
    public void append(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Object result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinName, jsonPathObject);
            Object queryResult = JsonPathQuery.append(jsonPathObject, result, jsonObject);
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, queryResult, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.append(writePolicy, documentKey, documentBinName, jsonPath, jsonObject, jsonPathObject);
        }
    }

    @Override
    public void append(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinNames, jsonPathObject);
            Map<String, Object> queryResults = new HashMap<>();
            for (String binName : result.keySet()) {
                queryResults.put(binName, JsonPathQuery.append(jsonPathObject, result.get(binName), jsonObject));
            }
            aerospikeDocumentRepository.put(writePolicy, documentKey, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.append(writePolicy, documentKey, documentBinNames, jsonPath, jsonObject, jsonPathObject);
        }
    }

    @Override
    public void delete(Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Object result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinName, jsonPathObject);
            Object queryResult = JsonPathQuery.delete(jsonPathObject, result);
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, queryResult, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.delete(writePolicy, documentKey, documentBinName, jsonPathObject);
        }
    }

    @Override
    public void delete(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinNames, jsonPathObject);
            Map<String, Object> queryResults = new HashMap<>();
            for (String binName : result.keySet()) {
                queryResults.put(binName, JsonPathQuery.delete(jsonPathObject, result.get(binName)));
            }
            aerospikeDocumentRepository.put(writePolicy, documentKey, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.delete(writePolicy, documentKey, documentBinNames, jsonPathObject);
        }
    }

    @Override
    public List<BatchRecord> batchPerform(List<BatchOperation> batchOperations, boolean parallel) throws DocumentApiException {
//        Map<Key, List<BatchOperation>> groupsByKey = getBatchOpStream(batchOperations, parallel)
//                .collect(Collectors.groupingBy(BatchOperation::getKey));
//        Map<Key, List<BatchOperation>> sameKeyGroups = groupsByKey.entrySet().stream()
//                .filter(entry -> entry.getValue().size() > 1)
//                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        int index = 0;
        while (index < batchOperations.size()) {
            List<BatchRecord> firstStepRecords = new ArrayList<>();
            List<BatchOperation> sublistBatchOps = new ArrayList<>();
            Map<Key, Boolean> keysRepeating = new HashMap<>();

            while (index < batchOperations.size()) {
                BatchOperation batchOp = batchOperations.get(index);

                if (batchOp.getBatchRecord() != null) { // if there is 1st step operation record
                    if (keysRepeating.get(batchOp.getKey()) != null
                            && keysRepeating.get(batchOp.getKey())) { // if its key is repeating
                        break; // breaking out after detecting the 1st repeating key
                    } else {
                        firstStepRecords.add(batchOp.getBatchRecord()); // collect 1st step operation record
                        keysRepeating.put(batchOp.getKey(), true); // update map
                    }
                }

                sublistBatchOps.add(batchOp); // collecting batch operations to a sublist until the 1st repeating key
                index++;
            }

            // performing first step operations
            if (!firstStepRecords.isEmpty()) {
                aerospikeDocumentRepository.batchPerform(batchPolicy, firstStepRecords);
            }

            // collecting non-empty second step records without json parsing error
            List<BatchRecord> secondStepRecords = getBatchOpStream(sublistBatchOps, parallel)
                    .map(BatchOperation::setSecondStepRecordAndGet)
                    .filter(Objects::nonNull)
                    .filter(batchRec -> batchRec.resultCode != -2)
                    .collect(Collectors.toList());

            // performing second step operations
            if (!secondStepRecords.isEmpty()) {
                aerospikeDocumentRepository.batchPerform(batchPolicy, secondStepRecords);
            }
        }

        // collecting resulting records
        return getBatchOpStream(batchOperations, parallel)
                .map(BatchOperation::getBatchRecord)
                .collect(Collectors.toList());
    }

    private Stream<BatchOperation> getBatchOpStream(List<BatchOperation> batchOperations, boolean parallel) {
        Stream<BatchOperation> batchOpStream = batchOperations.stream();
        if (parallel) batchOpStream = batchOpStream.parallel();

        return batchOpStream;
    }
}
