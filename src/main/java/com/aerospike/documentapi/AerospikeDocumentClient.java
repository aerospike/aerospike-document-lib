package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.data.DocumentFilter;
import com.aerospike.documentapi.data.DocumentFilterExp;
import com.aerospike.documentapi.data.DocumentQueryStatement;
import com.aerospike.documentapi.data.DocumentFilterSecIndex;
import com.aerospike.documentapi.data.KeyResult;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.JsonPathQuery;
import com.aerospike.documentapi.policy.DocumentPolicy;
import com.aerospike.documentapi.util.Lut;
import com.fasterxml.jackson.databind.JsonNode;
import net.minidev.json.JSONArray;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Primary object for accessing and mutating documents.
 */
public class AerospikeDocumentClient implements IAerospikeDocumentClient {

    private final AerospikeDocumentRepository aerospikeDocumentRepository;
    private final Policy readPolicy;
    private final WritePolicy writePolicy;
    private final BatchPolicy batchPolicy;
    private final QueryPolicy queryPolicy;

    public AerospikeDocumentClient(IAerospikeClient client) {
        this.aerospikeDocumentRepository = new AerospikeDocumentRepository(client);
        this.readPolicy = client.getReadPolicyDefault();
        this.writePolicy = client.getWritePolicyDefault();
        this.batchPolicy = client.getBatchPolicyDefault();
        this.queryPolicy = client.getQueryPolicyDefault();
    }

    public AerospikeDocumentClient(IAerospikeClient client, DocumentPolicy documentPolicy) {
        this.aerospikeDocumentRepository = new AerospikeDocumentRepository(client);
        this.readPolicy = documentPolicy.getReadPolicy();
        this.writePolicy = documentPolicy.getWritePolicy();
        this.batchPolicy = documentPolicy.getBatchPolicy();
        this.queryPolicy = documentPolicy.getQueryPolicy();
    }

    @Override
    public Object get(Key key, String binName, String jsonPath) {
        return get(key, Collections.singletonList(binName), jsonPath).get(binName);
    }

    @Override
    public Map<String, Object> get(Key key, Collection<String> binNames, String jsonPath) {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Map<String, Object> result = aerospikeDocumentRepository.get(readPolicy, key,
                binNames, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            result.replaceAll((k, v) -> JsonPathQuery.read(jsonPathObject, v));
        }
        return result;
    }

    @Override
    public void put(Key key, String binName, JsonNode jsonNode) {
        aerospikeDocumentRepository.put(writePolicy, key, binName, jsonNode);
    }

    @Override
    public void put(Key key, String binName, String jsonPath, Object object) {
        put(key, Collections.singletonList(binName), jsonPath, object);
    }

    @Override
    public void put(Key key, Collection<String> binNames, String jsonPath, Object object) {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, key,
                    binNames, jsonPathObject, true);
            Map<String, Object> queryResults = result.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(Lut.LUT_BIN))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> JsonPathQuery.putOrSet(jsonPathObject, entry.getValue(), object))
                    );
            aerospikeDocumentRepository.put(getLutPolicy(result), key, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.put(writePolicy, key, binNames, object, jsonPathObject);
        }
    }

    @Override
    public void append(Key key, String binName, String jsonPath, Object object) {
        append(key, Collections.singletonList(binName), jsonPath, object);
    }

    @Override
    public void append(Key key, Collection<String> binNames, String jsonPath, Object object) {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, key,
                    binNames, jsonPathObject, true);
            Map<String, Object> queryResults = result.entrySet().stream()
                    .filter(e -> !e.getKey().equals(Lut.LUT_BIN))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> JsonPathQuery.append(jsonPathObject, e.getValue(), object))
                    );
            aerospikeDocumentRepository.put(getLutPolicy(result), key, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.append(writePolicy, key, binNames, jsonPath,
                    object, jsonPathObject);
        }
    }

    @Override
    public void delete(Key key, String binName, String jsonPath) {
        delete(key, Collections.singletonList(binName), jsonPath);
    }

    @Override
    public void delete(Key key, Collection<String> binNames, String jsonPath) {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, key,
                    binNames, jsonPathObject, true);
            Map<String, Object> queryResults = result.entrySet().stream()
                    .filter(e -> !e.getKey().equals(Lut.LUT_BIN))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> JsonPathQuery.delete(jsonPathObject, e.getValue()))
                    );
            aerospikeDocumentRepository.put(getLutPolicy(result), key, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.delete(writePolicy, key, binNames, jsonPathObject);
        }
    }

    @Override
    public List<BatchRecord> batchPerform(List<BatchOperation> batchOperations, boolean parallel) {
        Map<Key, List<BatchOperation>> sameKeyGroups = groupByKeys(batchOperations);

        // validating and collecting first step operations
        List<BatchRecord> firstStepRecords = getBatchOpStream(batchOperations, parallel)
                .map(BatchOperation::getBatchRecord)
                .filter(Objects::nonNull)
                .map(batchRecord -> validateAndReturn(sameKeyGroups, batchRecord))
                .collect(Collectors.toList());

        // performing first step operations
        if (!firstStepRecords.isEmpty()) {
            aerospikeDocumentRepository.batchPerform(batchPolicy, firstStepRecords);
        }

        // collecting non-empty second step records without json parsing error
        List<BatchRecord> secondStepRecords = getBatchOpStream(batchOperations, parallel)
                .map(BatchOperation::setSecondStepRecordAndGet)
                .filter(Objects::nonNull)
                .filter(batchRec -> batchRec.resultCode != -2)
                .collect(Collectors.toList());

        // performing second step operations
        if (!secondStepRecords.isEmpty()) {
            aerospikeDocumentRepository.batchPerform(batchPolicy, secondStepRecords);
        }

        // collecting resulting records
        return getBatchOpStream(batchOperations, parallel)
                .map(BatchOperation::getBatchRecord)
                .collect(Collectors.toList());
    }

    @Override
    public Stream<KeyResult> query(DocumentQueryStatement queryStatement, DocumentFilter... docFilters) {
        QueryPolicy policy = new QueryPolicy(queryPolicy);
        policy.filterExp = getFilterExp(docFilters);

        Filter secIndexFilter = getSecIndexFilter(docFilters);
        Stream<KeyRecord> keyRecords = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                aerospikeDocumentRepository.query(policy, queryStatement.toStatement(secIndexFilter)).iterator(),
                Spliterator.ORDERED
        ), false);

        // no need to parse if there is no jsonPath given
        if (queryStatement.getJsonPaths() == null || queryStatement.getJsonPaths().length == 0) {
            return keyRecords.map(keyRecord -> new KeyResult(keyRecord.key, keyRecord.record));
        }

        // parsing KeyRecords to return the required objects
        return keyRecords
                .map(keyRecord -> getKeyResult(keyRecord.key,
                        getResults(queryStatement.getJsonPaths(), keyRecord.record.bins)))
                .filter(Objects::nonNull);
    }

    private Filter getSecIndexFilter(DocumentFilter[] docFilters) {
        if (docFilters == null || docFilters.length == 0) return null;

        return Arrays.stream(docFilters)
                .filter(Objects::nonNull)
                .filter(DocumentFilterSecIndex.class::isInstance)
                .map(filterExp -> ((DocumentFilterSecIndex) filterExp).toSecIndexFilter())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    private KeyResult getKeyResult(Key key, Map<String, Object> results) {
        return results.isEmpty() ? null : new KeyResult(key, results);
    }

    private Expression getFilterExp(DocumentFilter[] docFilters) {
        if (docFilters == null || docFilters.length == 0) return null;

        List<Exp> filterExps = Arrays.stream(docFilters)
                .filter(Objects::nonNull)
                .filter(DocumentFilterExp.class::isInstance)
                .map(filterExp -> ((DocumentFilterExp) filterExp).toFilterExp())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (filterExps.isEmpty()) return null;

        Exp expResult = filterExps.size() == 1 ?
                filterExps.get(0)
                : Exp.and(filterExps.toArray(new Exp[0]));
        return Exp.build(expResult);
    }

    private Map<String, Object> getResults(String[] jsonPaths, Map<String, Object> bins) {
        if (jsonPaths == null || jsonPaths.length == 0) return Collections.emptyMap();

        Map<String, Object> res = new HashMap<>();
        bins.values()
                .forEach(binValue -> Arrays.stream(jsonPaths)
                        .forEach(jsonPath -> addNonNull(jsonPath, JsonPathQuery.read(binValue, jsonPath), res))
                );
        return res;
    }

    private void addNonNull(String jsonPath, Object readRes, Map<String, Object> res) {
        if (readRes == null || (readRes instanceof JSONArray && ((JSONArray) readRes).isEmpty())) return;

        res.put(jsonPath, readRes);
    }

    private WritePolicy getLutPolicy(Map<String, Object> result) {
        return Lut.setLutPolicy(new WritePolicy(writePolicy), (long) result.get(Lut.LUT_BIN));
    }

    private Map<Key, List<BatchOperation>> groupByKeys(List<BatchOperation> batchOperations) {
        Map<Key, List<BatchOperation>> opsByKey = getBatchOpStream(batchOperations, true)
                .collect(Collectors.groupingBy(BatchOperation::getKey));

        return opsByKey.entrySet().parallelStream()
                .filter(entry -> entry.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Stream<BatchOperation> getBatchOpStream(List<BatchOperation> batchOperations, boolean parallel) {
        Stream<BatchOperation> batchOpStream = batchOperations.stream();
        if (parallel) batchOpStream = batchOpStream.parallel();

        return batchOpStream;
    }

    private BatchRecord validateAndReturn(Map<Key, List<BatchOperation>> sameKeyGroups, BatchRecord batchRecord) {
        if (sameKeyGroups.containsKey(batchRecord.key)) {
            throw new IllegalArgumentException("Multiple two-step operations with the same key are not allowed");
        }
        return batchRecord;
    }
}
