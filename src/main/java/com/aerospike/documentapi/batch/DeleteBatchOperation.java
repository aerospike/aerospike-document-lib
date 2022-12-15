package com.aerospike.documentapi.batch;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.documentapi.jsonpath.JsonPathQuery;
import com.aerospike.documentapi.jsonpath.PathDetails;
import com.aerospike.documentapi.util.Lut;

import java.util.Collection;
import java.util.Map;

import static com.aerospike.documentapi.util.Utils.getPathDetails;

public class DeleteBatchOperation extends AbstractBatchOperation {

    public DeleteBatchOperation(Key key, Collection<String> binNames, String jsonPath) {
        super(key, binNames, jsonPath);
    }

    @Override
    protected Object firstStepJsonPathQuery(Map.Entry<String, Object> entry) {
        return JsonPathQuery.delete(jsonPathObject, entry.getValue());
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
                final PathDetails pathDetails = getPathDetails(originalJsonPathObject.getPathParts(), true);
                batchOps = firstStepQueryResults().entrySet().stream()
                        .map(entry -> pathDetails.getFinalPathPart()
                                .toAerospikePutOperation(entry.getKey(), entry.getValue(), pathDetails.getCtxArray()))
                        .toArray(Operation[]::new);
            } else {
                final PathDetails pathDetails = getPathDetails(jsonPathObject.getPathParts(), true);
                batchOps = binNames.stream()
                        .map(binName -> pathDetails.getFinalPathPart()
                                .toAerospikeDeleteOperation(binName, pathDetails.getCtxArray()))
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
}
