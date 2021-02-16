package com.aerospike.documentAPI;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;

import java.util.Map;

public interface IAerospikeDocumentClient {
    /**
     * Retrieve the object in the document with key documentKey that is referenced by the Json path
     *
     * @param documentKey
     * @param jsonPath
     * @return Object referenced by jsonPath
     */
    Object get(Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

    /**
     * Put a document
     *
     * @param documentKey - document key
     * @param jsonObject  - document
     */
    void put(Key documentKey, Map jsonObject);

    /**
     * Put a map representation of a JSON object at a particular path in a json document
     *
     * @param documentKey
     * @param jsonPath
     * @param jsonObject
     */
    void put(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

    /**
     * Append an object to a list in a document specified by a json path
     *
     * @param documentKey
     * @param jsonPath
     * @param jsonObject
     */
    void append(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

    /**
     * Delete an object in a document specified by a json path
     *
     * @param documentKey
     * @param jsonPath
     * @throws JsonPathParser.JsonParseException
     * @throws AerospikeDocumentClient.AerospikeDocumentClientException
     */
    void delete(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;
}