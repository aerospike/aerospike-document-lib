package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IAerospikeDocumentRepository {

    Map<String, Object> get(Policy readPolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject);

    Map<String, Object> get(Policy readPolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject,
                            boolean withLut);

    void put(WritePolicy writePolicy, Key key, String binName, JsonNode jsonNode);

    void put(WritePolicy writePolicy, Key key, String binName, Map<?, ?> jsonMap);

    void put(WritePolicy writePolicy, Key key, Collection<String> binNames, Object jsonObject, JsonPathObject jsonPathObject);

    void put(WritePolicy writePolicy, Key key, Map<String, Object> queryResults, JsonPathObject jsonPathObject);

    void append(WritePolicy writePolicy, Key key, Collection<String> binNames, String jsonPath, Object jsonObject,
                JsonPathObject jsonPathObject);

    void delete(WritePolicy writePolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject);

    boolean batchPerform(BatchPolicy batchPolicy, List<BatchRecord> batchRecords);
}
