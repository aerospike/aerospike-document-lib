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
import java.util.Objects;

import static com.aerospike.documentapi.util.Utils.getPathDetails;

public class PutBatchOperation extends AbstractBatchOperation {

    private final Object objToPut;

    public PutBatchOperation(Key key, Collection<String> binNames, String jsonPath, Object objectToPut) {
        super(key, binNames, jsonPath);
        this.objToPut = objectToPut;
    }

    @Override
    protected Object firstStepJsonPathQuery(Map.Entry<String, Object> entry) {
        return JsonPathQuery.putOrSet(jsonPathObject, entry.getValue(), objToPut);
    }

    @Override
    public BatchRecord setSecondStepRecordAndGet() {
        Operation[] batchOps;

        if (originalJsonPathObject.getTokensNotRequiringSecondStepQuery().isEmpty()) {
            // If there are no parts, put an empty map in the given bin
            batchOps = getBinNames().stream()
                    .map(MapOperation::clear)
                    .toArray(Operation[]::new);
        } else {
            if (isRequiringJsonPathQuery()) {
                // using the original object as the initially parsed one has already been changed within the 1st step
                final PathDetails pathDetails =
                        getPathDetails(originalJsonPathObject.getTokensNotRequiringSecondStepQuery(), true);
                batchOps = firstStepQueryResults().entrySet().stream()
                        .map(entry -> toPutOperation(entry.getKey(), entry.getValue(), pathDetails))
                        .filter(Objects::nonNull)
                        .toArray(Operation[]::new);
            } else {
                final PathDetails pathDetails = getPathDetails(jsonPathObject.getTokensNotRequiringSecondStepQuery(),
                        true);
                batchOps = binNames.stream()
                        .map(binName -> toPutOperation(binName, objToPut, pathDetails))
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
}
