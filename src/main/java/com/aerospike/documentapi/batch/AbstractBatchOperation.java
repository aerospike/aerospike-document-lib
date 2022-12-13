package com.aerospike.documentapi.batch;

import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.pathpart.PathPart;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public abstract class AbstractBatchOperation implements BatchOperation {

    protected final Key key;
    protected final Collection<String> binNames;
    protected final String jsonPath;
    protected final JsonPathObject jsonPathObject;
    protected final JsonPathObject originalJsonPathObject;
    protected final boolean requiringJsonPathQuery;
    protected BatchRecord batchRecord;
    protected String errorBinName;

    protected AbstractBatchOperation(Key key, Collection<String> binNames, String jsonPath) {
        this.key = key;
        this.binNames = binNames;
        this.jsonPath = jsonPath;
        try {
            this.jsonPathObject = new JsonPathParser().parse(jsonPath);
        } catch (JsonPathParser.JsonParseException e) {
            throw new IllegalArgumentException(e);
        }
        this.requiringJsonPathQuery = jsonPathObject.requiresJsonPathQuery();
        // copying in order to have it for a second step
        this.originalJsonPathObject = jsonPathObject.copy();
        if (jsonPathObject.requiresJsonPathQuery()) {
            setFirstStepRecord();
        }
    }

    @Override
    public void setFirstStepRecord() {
        Operation[] batchOperations;
        final PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts());
        batchOperations = binNames.stream()
                .map(binName -> pathDetails.getFinalPathPart()
                        .toAerospikeGetOperation(binName, pathDetails.getCtxArray()))
                .toArray(Operation[]::new);

        batchRecord = new BatchRead(key, batchOperations);
    }

    protected PathDetails getPathDetails(List<PathPart> pathParts) {
        // We need to treat the last part of the path differently
        PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathParts);
        // Then turn the rest into the contexts representation
        CTX[] ctxArray = JsonPathParser.pathPartsToContextArray(pathParts);

        return new PathDetails(finalPathPart, ctxArray);
    }

    protected Map<String, Object> firstStepQueryResults() {
        return null; // is implemented by some child classes
    }

    protected Operation toPutOperation(String binName, Object objToPut, PathDetails pathDetails) {
        try {
            return pathDetails.getFinalPathPart()
                    .toAerospikePutOperation(binName, objToPut, pathDetails.getCtxArray());
        } catch (IllegalArgumentException e) {
            errorBinName = binName;
            return null;
        }
    }

    protected BatchRecord getErrorBatchWriteRecord() {
        // empty first step query results will cause AerospikeException for the whole batch
        // from the client as it tries to perform an empty write operation
        Map<String, Object> bins = new HashMap<>();

        if (errorBinName != null && !errorBinName.isEmpty()) {
            bins.put(errorBinName, originalJsonPathObject);
        } else {
            // in this case we don`t know a specific bin that caused parsing error
            binNames.forEach(binName -> bins.put(binName, originalJsonPathObject));
        }

        Record record;
        if (batchRecord != null && batchRecord.record != null) {
            record = new Record(bins, batchRecord.record.generation, batchRecord.record.expiration);
        } else {
            record = new Record(bins, 0, 0);
        }

        return new BatchRecord(key, record, -2, false, true);
    }

    @Value
    @RequiredArgsConstructor
    protected static class PathDetails {
        PathPart finalPathPart;
        CTX[] ctxArray;
    }

    @Value
    @RequiredArgsConstructor
    protected static class Bin {
        String name;
        Object value;
    }
}