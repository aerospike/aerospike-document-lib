package com.aerospike.documentapi;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.pathparts.PathPart;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

class AerospikeDocumentRepository {
    public static String DEFAULT_DOCUMENT_BIN_NAME = "documentBin";

    private final IAerospikeClient client;
    private String documentBinName = DEFAULT_DOCUMENT_BIN_NAME;

    AerospikeDocumentRepository(IAerospikeClient client) {
        this.client = client;
    }

    AerospikeDocumentRepository(IAerospikeClient client, String documentBinName) {
        this.client = client;
        this.documentBinName = documentBinName;
    }

    Object get(Policy readPolicy, Key documentKey, JsonPathObject jsonPathObject) throws DocumentApiException {
        // If there are no parts, retrieve the full document
        if (jsonPathObject.getPathParts().size() == 0) {
            return client.get(readPolicy, documentKey).getValue(documentBinName);
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
            return r.getValue(documentBinName);
        }
    }

    void put(WritePolicy writePolicy, Key documentKey, JsonNode jsonNode) {
        client.put(writePolicy, documentKey, Utils.createBinByJsonNodeType(documentBinName, jsonNode));
    }

    void put(WritePolicy writePolicy, Key documentKey, Object jsonObject, JsonPathObject jsonPathObject) throws DocumentApiException {
        // If there are no parts, put the full document
        if (jsonPathObject.getPathParts().size() == 0) {
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

    void append(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, you can't append
        if (jsonPathObject.getPathParts().size() == 0) {
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

    void delete(WritePolicy writePolicy, Key documentKey, String jsonPath, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException {
        // If there are no parts, you can't append
        if (jsonPathObject.getPathParts().size() == 0) {
            throw new JsonPathParser.ListException(jsonPath);
        } else {
            List<PathPart> pathPart = jsonPathObject.getPathParts();
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPart);
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPart);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeDeleteOperation(documentBinName, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }
}