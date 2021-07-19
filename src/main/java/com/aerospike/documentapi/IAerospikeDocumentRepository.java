package com.aerospike.documentapi;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;

public interface IAerospikeDocumentRepository {

    Object get(Policy readPolicy, Key documentKey, String documentBinName, JsonPathObject jsonPathObject) throws DocumentApiException;

    Map<String, Object> get(Policy readPolicy, Key documentKey, List<String> documentBinNames, JsonPathObject jsonPathObject) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode);

    void put(WritePolicy writePolicy, Key documentKey, String documentBinName, Object jsonObject, JsonPathObject jsonPathObject) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, Object jsonObject, JsonPathObject jsonPathObject) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, List<Object> jsonPathQueryResults, JsonPathObject jsonPathObject) throws DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, String jsonPath, Object jsonObject, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, String jsonPath, List<Object> jsonPathQueryResults, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void delete(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;

    void delete(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, String jsonPath, JsonPathObject jsonPathObject) throws JsonPathParser.ListException, DocumentApiException;
}
