package com.aerospike.documentapi.batch;

import com.aerospike.client.BatchRead;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.documentapi.jsonpath.JsonPathQuery;
import net.minidev.json.JSONArray;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Batch GET operation has only the first step and post-production
 */
public class GetBatchOperation extends AbstractBatchOperation {

    public GetBatchOperation(Key key, Collection<String> binNames, String jsonPath) {
        super(key, binNames, jsonPath);
    }

    @Override
    public BatchRecord setSecondStepRecordAndGet() {
        if (originalJsonPathObject.getPathParts().isEmpty()) {
            batchRecord = new BatchRead(key, binNames.stream().map(Operation::get).toArray(Operation[]::new));
        } else {
            if (isRequiringJsonPathQuery()) {
                return processQueryResults();
            } else {
                // 1st step serves as the second
                // as GET operation has only one step and post-production
                setFirstStepRecord();
            }
        }

        return batchRecord;
    }

    @Override
    protected Collection<Operation> readOperations() {
        return Collections.emptyList();
    }

    private BatchRecord processQueryResults() {
        if (batchRecord != null && batchRecord.record.bins != null) {
            Map<String, Object> bins = new HashMap<>();

            for (Map.Entry<String, Object> entry : batchRecord.record.bins.entrySet()) {
                Object res;

                try {
                    res = JsonPathQuery.read(originalJsonPathObject, entry.getValue());
                } catch (Exception e) {
                    return batchRecordWithError(entry.getKey());
                }

                if (res instanceof JSONArray && ((JSONArray) res).isEmpty()) {
                    return batchRecordWithError(entry.getKey());
                } else {
                    bins.put(entry.getKey(), res);
                }
            }

            batchRecord.record = new Record(
                    bins,
                    batchRecord.record.generation,
                    batchRecord.record.expiration
            );

            // this way the record will be filtered out before getting processed again
            // as it should perform only the first step and post-production
            return null;
        } else {
            batchRecord = getErrorBatchWriteRecord();
            return batchRecord;
        }
    }

    private BatchRecord batchRecordWithError(String binName) {
        errorBinName = binName;
        batchRecord = getErrorBatchWriteRecord();
        return batchRecord;
    }
}
