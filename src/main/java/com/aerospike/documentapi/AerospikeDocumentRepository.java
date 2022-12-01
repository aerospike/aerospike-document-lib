package com.aerospike.documentapi;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.pathpart.PathPart;
import com.aerospike.documentapi.util.Utils;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;

class AerospikeDocumentRepository implements IAerospikeDocumentRepository {

    private final IAerospikeClient client;

    AerospikeDocumentRepository(IAerospikeClient client) {
        this.client = client;
    }

    @Override
    public Object get(Policy readPolicy, Key documentKey, String documentBinName, JsonPathObject jsonPathObject)
            throws DocumentApiException {
        // If there are no parts, retrieve the full document
        if (jsonPathObject.getPathParts().isEmpty()) {
            Record record = client.get(readPolicy, documentKey, documentBinName);

            if (record != null) {
                return record.getValue(documentBinName);
            }
        } else { // else retrieve using pure contexts
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            // Retrieve the part of the document referred to by the JSON path
            Record r;
            try {
                WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
                r = client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeGetOperation(documentBinName, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }

            if (r != null) {
                return r.getValue(documentBinName);
            }
        }
        return null;
    }

    @Override
    public Map<String, Object> get(Policy readPolicy, Key documentKey, Collection<String> documentBinNames,
                                   JsonPathObject jsonPathObject) throws DocumentApiException {
        Map<String, Object> results = new HashMap<>();
        // If there are no parts, retrieve the full document
        if (jsonPathObject.getPathParts().isEmpty()) {
            Record record = client.get(readPolicy, documentKey, documentBinNames.toArray(new String[0]));

            if (record != null) {
                for (String binName : documentBinNames) {
                    results.put(binName, record.bins.get(binName));
                }
            }
        } else { // else retrieve using pure contexts
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            // Retrieve the part of the document referred to by the JSON path
            Record r;
            Operation[] operations = documentBinNames.stream()
                    .map(bn -> finalPathPart.toAerospikeGetOperation(bn, ctxArray)).toArray(Operation[]::new);

            try {
                WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
                r = client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }

            if (r != null) {
                results.putAll(r.bins);
            }
        }
        return results;
    }

    @Override
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode) {
        client.put(writePolicy, documentKey, Utils.createBinByJsonNodeType(documentBinName, jsonNode));
    }

    @Override
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, Object jsonObject,
                    JsonPathObject jsonPathObject) throws DocumentApiException {
        // If there are no parts, put the full document
        if (jsonPathObject.getPathParts().isEmpty()) {
            client.put(writePolicy, documentKey, new Bin(documentBinName, jsonObject));
        } else { // else put using contexts
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikePutOperation(documentBinName, jsonObject, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
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
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                operations = documentBinNames.stream()
                        .map(bn -> finalPathPart.toAerospikePutOperation(bn, jsonObject, ctxArray))
                        .toArray(Operation[]::new);
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
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                operations = queryResults.entrySet().stream()
                        .map(e -> finalPathPart.toAerospikePutOperation(e.getKey(), e.getValue(), ctxArray))
                        .toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void append(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject,
                       JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, you can't append
        if (jsonPathObject.getPathParts().isEmpty()) {
            throw new JsonPathParser.ListException(jsonPath);
        } else {
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPart(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeAppendOperation(documentBinName, jsonObject, ctxArray));
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
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPart(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                Operation[] operations = documentBinNames.stream()
                        .map(bn -> finalPathPart.toAerospikeAppendOperation(bn, jsonObject, ctxArray))
                        .toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void append(WritePolicy writePolicy, Key documentKey, Map<String, Object> queryResults, String jsonPath,
                       JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, you can't append
        if (jsonPathObject.getPathParts().isEmpty()) {
            throw new JsonPathParser.ListException(jsonPath);
        } else {
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPart(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                Operation[] operations = queryResults.entrySet().stream()
                        .map(e -> finalPathPart.toAerospikeAppendOperation(e.getKey(), e.getValue(), ctxArray))
                        .toArray(Operation[]::new);
                client.operate(writePolicy, documentKey, operations);
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }

    @Override
    public void delete(WritePolicy writePolicy, Key documentKey, String documentBinName,
                       JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, put an empty map in the given bin
        if (jsonPathObject.getPathParts().isEmpty()) {
            Map<String, Object> emptyMap = new HashMap<>();
            client.put(writePolicy, documentKey, new Bin(documentBinName, emptyMap));
        } else {
            List<PathPart> pathParts = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathParts);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathParts);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeDeleteOperation(documentBinName, ctxArray));
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
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                Operation[] operations = documentBinNames.stream()
                        .map(bn -> finalPathPart.toAerospikeDeleteOperation(bn, ctxArray))
                        .toArray(Operation[]::new);
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
