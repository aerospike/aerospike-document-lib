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

    Object get(Policy readPolicy, Key documentKey, String documentBinName, JsonPathObject jsonPathObject) throws DocumentApiException;

    Map<String, Object> get(Policy readPolicy, Key documentKey, Collection<String> documentBinNames, JsonPathObject jsonPathObject) throws DocumentApiException;

    Map<String, Object> get(Policy readPolicy, Key documentKey, Collection<String> documentBinNames, JsonPathObject jsonPathObject, boolean withLut) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode);

    void put(WritePolicy writePolicy, Key documentKey, String documentBinName, Object jsonObject, JsonPathObject jsonPathObject) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, Object jsonObject, JsonPathObject jsonPathObject) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, Map<String, Object> queryResults, JsonPathObject jsonPathObject) throws DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, Map<String, Object> queryResults, String jsonPath, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void delete(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void delete(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    boolean batchPerform(BatchPolicy batchPolicy, List<BatchRecord> batchRecords) throws DocumentApiException;
}
