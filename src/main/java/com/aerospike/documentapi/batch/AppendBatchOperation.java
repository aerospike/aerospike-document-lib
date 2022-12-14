package com.aerospike.documentapi.batch;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.JsonPathQuery;
import com.aerospike.documentapi.jsonpath.pathpart.PathPart;
import com.aerospike.documentapi.util.Lut;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPathException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        CTX[] ctxArray = JsonPathParser.pathPartsToContextArray(pathParts);

        return new PathDetails(finalPathPart, ctxArray);
    }

    @Override
    protected Object firstStepJsonPathQuery(Map.Entry<String, Object> entry)
            throws JsonProcessingException, JsonPathException {
        return JsonPathQuery.append(jsonPathObject, entry.getValue(), objToAppend);
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
                        .map(entry -> toPutOperation(entry.getKey(), entry.getValue(), pathDetails))
                        .filter(Objects::nonNull)
                        .toArray(Operation[]::new);
            } else {
                final PathDetails pathDetails = getPathDetailsForAppend(jsonPathObject.getPathParts()); // needs to be treated without modifying
                batchOps = binNames.stream()
                        .map(binName -> toAppendOperation(binName, objToAppend, pathDetails))
                        .filter(Objects::nonNull)
                        .toArray(Operation[]::new);
            }
        }

        if (batchOps.length > 0) {
            batchRecord = new BatchWrite(
                    getLutValue().map(v -> Lut.setLutPolicy(new BatchWritePolicy(), v)).orElse(null),
                    key,
                    batchOps
            );
        } else {
            batchRecord = getErrorBatchWriteRecord();
        }

        return batchRecord;
    }

    protected Operation toAppendOperation(String binName, Object objToAppend, PathDetails pathDetails) {
        try {
            return pathDetails.getFinalPathPart()
                    .toAerospikeAppendOperation(binName, objToAppend, pathDetails.getCtxArray());
        } catch (IllegalArgumentException e) {
            errorBinName = binName;
            return null;
        }
    }
}
