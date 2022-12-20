package com.aerospike.documentapi.batch;

import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.PathDetails;
import com.aerospike.documentapi.util.Lut;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.aerospike.documentapi.util.Utils.getPathDetails;

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
        this.jsonPathObject = new JsonPathParser().parse(jsonPath);
        this.requiringJsonPathQuery = jsonPathObject.requiresJsonPathQuery();
        // copying in order to have it for a second step
        this.originalJsonPathObject = jsonPathObject.copy();
        if (jsonPathObject.requiresJsonPathQuery()) {
            setFirstStepRecord();
        }
    }

    @Override
    public void setFirstStepRecord() {
        final PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), true);
        List<Operation> batchOperations = binNames.stream()
                .map(binName -> pathDetails.getFinalPathPart()
                        .toAerospikeGetOperation(binName, pathDetails.getCtxArray()))
                .collect(Collectors.toList());

        batchOperations.addAll(readOperations());
        batchRecord = new BatchRead(key, batchOperations.toArray(new Operation[0]));
    }

    protected Collection<Operation> readOperations() {
        return Collections.singleton(Lut.LUT_READ_OP);
    }

    protected Optional<Long> getLutValue() {
        return Objects.isNull(batchRecord)
                ? Optional.empty()
                : Optional.of(batchRecord.record.getLong(Lut.LUT_BIN));
    }

    protected Map<String, Object> firstStepQueryResults() {
        Map<String, Object> resultingMap = new HashMap<>();

        if (batchRecord.record != null && batchRecord.record.bins != null) {
            for (Map.Entry<String, Object> entry : batchRecord.record.bins.entrySet()) {
                if (entry.getKey().equals(Lut.LUT_BIN)) continue;
                Object res;
                try {
                    res = firstStepJsonPathQuery(entry);
                } catch (Exception e) {
                    errorBinName = entry.getKey();
                    return Collections.emptyMap();
                }
                resultingMap.put(entry.getKey(), res);
            }
        }
        return resultingMap;
    }

    protected Object firstStepJsonPathQuery(Map.Entry<String, Object> entry) {
        throw new UnsupportedOperationException("Not implemented");
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
    protected static class Bin {
        String name;
        Object value;
    }
}
