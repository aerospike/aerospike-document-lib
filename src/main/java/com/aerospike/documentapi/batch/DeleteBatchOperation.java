package com.aerospike.documentapi.batch;

import com.aerospike.client.*;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.documentapi.JsonPathQuery;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPathException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DeleteBatchOperation extends AbstractBatchOperation {

    public DeleteBatchOperation(Key key, Collection<String> binNames, String jsonPath) {
        super(key, binNames, jsonPath);
    }

    @Override
    protected Map<String, Object> firstStepQueryResults() {
        Map<String, Object> resultingMap = new HashMap<>();

        if (batchRecord.record != null && batchRecord.record.bins != null) {
            for (Map.Entry<String, Object> entry : batchRecord.record.bins.entrySet()) {
                Object res;

                try {
                    res = JsonPathQuery.delete(jsonPathObject, entry.getValue());
                } catch (JsonProcessingException | JsonPathException e) {
                    errorBinName = entry.getKey();
                    return new HashMap<>();
                }

                resultingMap.put(entry.getKey(), res);
            }
        }

        return resultingMap;
    }

    @Override
    public BatchRecord setSecondStepRecordAndGet() {
        Operation[] batchOps;

        if (originalJsonPathObject.getPathParts().isEmpty()) {
            // If there are no parts, put an empty map in the given bin
            batchOps = getBinNames().stream()
                    .map(MapOperation::clear)
                    .toArray(Operation[]::new);
        } else {
            if (isRequiringJsonPathQuery()) {
                // using the original object as the initially parsed one has already been changed within the 1st step
                final PathDetails pathDetails = getPathDetails(originalJsonPathObject.getPathParts());
                batchOps = firstStepQueryResults().entrySet().stream()
                        .map(entry -> pathDetails.getFinalPathPart()
                                .toAerospikePutOperation(entry.getKey(), entry.getValue(), pathDetails.getCtxArray()))
                        .toArray(Operation[]::new);
            } else {
                final PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts());
                batchOps = binNames.stream()
                        .map(binName -> pathDetails.getFinalPathPart()
                                .toAerospikeDeleteOperation(binName, pathDetails.getCtxArray()))
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
