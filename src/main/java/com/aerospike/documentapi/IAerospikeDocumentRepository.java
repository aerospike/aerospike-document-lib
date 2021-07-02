package com.aerospike.documentapi;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.JsonNode;

public interface IAerospikeDocumentRepository {

    Object get(Policy readPolicy, Key documentKey, JsonPathObject jsonPathObject, String documentBinName) throws DocumentApiException;

    void put(WritePolicy writePolicy, Key documentKey, JsonNode jsonNode, String documentBinName);

    void put(WritePolicy writePolicy, Key documentKey, Object jsonObject, JsonPathObject jsonPathObject, String documentBinName) throws DocumentApiException;

    void append(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject, JsonPathObject jsonPathObject, String documentBinName) throws JsonPathParser.ListException, DocumentApiException;

    void delete(WritePolicy writePolicy, Key documentKey, String jsonPath, JsonPathObject jsonPathObject, String documentBinName) throws JsonPathParser.ListException, DocumentApiException;
}
