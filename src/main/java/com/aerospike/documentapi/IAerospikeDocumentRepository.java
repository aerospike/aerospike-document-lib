package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IAerospikeDocumentRepository {

    Map<String, Object> get(Policy readPolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject)
            throws DocumentApiException;

    Map<String, Object> get(Policy readPolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject,
                            boolean withLut) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key key, String binName, JsonNode jsonNode);

    void put(WritePolicy writePolicy, Key key, String binName, Map<?, ?> jsonMap);

    void put(WritePolicy writePolicy, Key key, Collection<String> binNames, Object jsonObject, JsonPathObject jsonPathObject)
            throws DocumentApiException;

    void put(WritePolicy writePolicy, Key key, Map<String, Object> queryResults, JsonPathObject jsonPathObject)
            throws DocumentApiException;

    void append(WritePolicy writePolicy, Key key, Collection<String> binNames, String jsonPath, Object jsonObject,
                JsonPathObject jsonPathObject) throws DocumentApiException;

    void delete(WritePolicy writePolicy, Key key, Collection<String> binNames, JsonPathObject jsonPathObject)
            throws DocumentApiException;

    boolean batchPerform(BatchPolicy batchPolicy, List<BatchRecord> batchRecords) throws DocumentApiException;
}