package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.PathDetails;
import com.aerospike.documentapi.util.Lut;
import com.aerospike.documentapi.util.Utils;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.aerospike.documentapi.util.Utils.getPathDetails;

class AerospikeDocumentRepository implements IAerospikeDocumentRepository {

    private final IAerospikeClient client;

    AerospikeDocumentRepository(IAerospikeClient client) {
        this.client = client;
    }

    @Override
    public Map<String, Object> get(Policy readPolicy, Key documentKey, Collection<String> documentBinNames,
                                   JsonPathObject jsonPathObject) throws DocumentApiException {
        return get(readPolicy, documentKey, documentBinNames, jsonPathObject, false);
    }

    @Override
    public Map<String, Object> get(Policy readPolicy, Key documentKey, Collection<String> documentBinNames,
                                   JsonPathObject jsonPathObject, boolean withLut) throws DocumentApiException {
        Map<String, Object> results = new HashMap<>();
        // If there are no parts, retrieve the full document
        if (jsonPathObject.getPathParts().isEmpty()) {
            List<Operation> operations = new ArrayList<>();
            for (String binName : documentBinNames) {
                operations.add(Operation.get(binName));
            }
            if (withLut) {
                operations.add(Lut.LUT_READ_OP);
            }
            WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
            Record rec = client.operate(writePolicy, documentKey, operations.toArray(new Operation[0]));
            if (rec != null) {
                results.putAll(rec.bins);
            }
        } else { // else retrieve using pure contexts
            PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), true);

            List<Operation> operations = documentBinNames.stream()
                    .map(binName -> pathDetails.getFinalPathPart().toAerospikeGetOperation(
                            binName,
                            pathDetails.getCtxArray())
                    ).collect(Collectors.toList());

            if (withLut) {
                operations.add(Lut.LUT_READ_OP);
            }

            Record rec;
            try {
                WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
                rec = client.operate(writePolicy, documentKey, operations.toArray(new Operation[0]));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
            if (rec != null) {
                results.putAll(rec.bins);
            }
        }
        return results;
    }

    @Override
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode) {
        client.put(writePolicy, documentKey, Utils.createBinByJsonNodeType(documentBinName, jsonNode));
    }

    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, Map<?,?> map) {
        client.put(writePolicy, documentKey, new Bin(documentBinName, map));
    }

    @Override
    public void put(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, Object jsonObject,
                    JsonPathObject jsonPathObject) throws DocumentApiException {
        Operation[] operations;
        // If there are no parts, put the full document
        if (jsonPathObject.getPathParts().isEmpty()) {
            operations = documentBinNames.stream()
                    .map(bn -> {
                        Bin bin = new Bin(bn, jsonObject);
                        return Operation.put(bin);
                    })
                    .toArray(Operation[]::new);
            client.operate(writePolicy, documentKey, operations);
        } else { // else put using contexts
            PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), true);

            try {
                operations = documentBinNames.stream()
                        .map(binName -> pathDetails.getFinalPathPart().toAerospikePutOperation(
                                binName,
                                jsonObject,
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void put(WritePolicy writePolicy, Key documentKey, Map<String, Object> queryResults,
                    JsonPathObject jsonPathObject) throws DocumentApiException {
        Operation[] operations;
        // If there are no parts, put the full document
        if (jsonPathObject.getPathParts().isEmpty()) {
            operations = queryResults.entrySet().stream()
                    .map(e -> {
                        Bin bin = new Bin(e.getKey(), e.getValue());
                        return Operation.put(bin);
                    })
                    .toArray(Operation[]::new);
            client.operate(writePolicy, documentKey, operations);
        } else { // else put using contexts
            PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), true);

            try {
                operations = queryResults.entrySet().stream()
                        .map(entry -> pathDetails.getFinalPathPart().toAerospikePutOperation(
                                entry.getKey(),
                                entry.getValue(),
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void append(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, String jsonPath,
                       Object jsonObject, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, you can't append
        if (jsonPathObject.getPathParts().isEmpty()) {
            throw new JsonPathParser.ListException(jsonPath);
        } else {
            PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), false);

            try {
                Operation[] operations = documentBinNames.stream()
                        .map(binName -> pathDetails.getFinalPathPart().toAerospikeAppendOperation(
                                binName,
                                jsonObject,
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void delete(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames,
                       JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, put an empty map in each given bin
        if (jsonPathObject.getPathParts().isEmpty()) {
            Operation[] operations = documentBinNames.stream()
                    .map(MapOperation::clear)
                    .toArray(Operation[]::new);
            client.operate(writePolicy, documentKey, operations);
        } else {
            PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), true);

            try {
                Operation[] operations = documentBinNames.stream()
                        .map(binName -> pathDetails.getFinalPathPart().toAerospikeDeleteOperation(
                                binName,
                                pathDetails.getCtxArray())
                        ).toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public boolean batchPerform(BatchPolicy batchPolicy, List<BatchRecord> batchRecords) throws DocumentApiException {
        try {
            return client.operate(batchPolicy, batchRecords);
        } catch (AerospikeException e) {
            throw new DocumentApiException(e);
        }
    }
}