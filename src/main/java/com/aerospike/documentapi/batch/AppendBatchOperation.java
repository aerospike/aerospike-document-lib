package com.aerospike.documentapi.batch;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.JsonPathParser;
import com.aerospike.documentapi.JsonPathQuery;
import com.aerospike.documentapi.pathparts.PathPart;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPathException;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppendBatchOperation extends AbstractBatchOperation {

    private final Object objToAppend;

    public AppendBatchOperation(Key key, Collection<String> binNames, String jsonPath, Object objectToPut) {
        super(key, binNames, jsonPath);
        this.objToAppend = objectToPut;
    }

    private PathDetails getPathDetailsForAppend(List<PathPart> pathParts) {
        // We need to treat the last part of the path differently
        PathPart finalPathPart = JsonPathParser.extractLastPathPart(pathParts);
        // Then turn the rest into the contexts representation
        CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathParts);

        return new PathDetails(finalPathPart, ctxArray);
    }

    @Override
    protected Map<String, Object> firstStepQueryResults() {
        Map<String, Object> resultingMap = new HashMap<>();

        if (batchRecord.record != null && batchRecord.record.bins != null) {
            for (Map.Entry<String, Object> entry : batchRecord.record.bins.entrySet()) {
                Object res;

                try {
                    res = JsonPathQuery.append(jsonPathObject, entry.getValue(), objToAppend);
                } catch (JsonProcessingException | JsonPathException e) {
                    errorBinName = entry.getKey();
                    return new HashMap<>();
                }

                resultingMap.put(entry.getKey(), res);
            }
        }

        return resultingMap;
    }

    public BatchRecord setSecondStepRecordAndGet() {
        Operation[] batchOps;

        if (originalJsonPathObject.getPathParts().isEmpty()) {
            // If there are no parts, you cannot append
            try {
                throw new JsonPathParser.ListException(getJsonPath());
            } catch (JsonPathParser.ListException e) {
                throw new AerospikeException(e);
            }
        } else {
            if (isRequiringJsonPathQuery()) {
                // using the original object as the initially parsed one has already been changed within the 1st step
                final PathDetails pathDetails = getPathDetails(originalJsonPathObject.getPathParts());
                batchOps = firstStepQueryResults().entrySet().stream()
                        .map(entry -> pathDetails.getFinalPathPart()
                                .toAerospikePutOperation(entry.getKey(), entry.getValue(), pathDetails.getCtxArray()))
                        .toArray(Operation[]::new);
            } else {
                final PathDetails pathDetails = getPathDetailsForAppend(jsonPathObject.getPathParts()); // needs to be treated without modifying
                batchOps = binNames.stream()
                        .map(binName -> pathDetails.getFinalPathPart()
                                .toAerospikeAppendOperation(binName, objToAppend, pathDetails.getCtxArray()))
                        .toArray(Operation[]::new);

            }
        }

        if (batchOps.length > 0) {
            batchRecord = new BatchWrite(key, batchOps);
        } else {
            batchRecord = getErrorBatchRecord();
        }

        return batchRecord;
    }
}