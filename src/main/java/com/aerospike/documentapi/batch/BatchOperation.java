package com.aerospike.documentapi.batch;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;

import java.util.Collection;

public interface BatchOperation {

    Key getKey();

    Collection<String> getBinNames();

    String getJsonPath();

    BatchRecord getBatchRecord();

    void setFirstStepRecord();

    BatchRecord setSecondStepRecordAndGet();
}