package com.aerospike.documentapi;

import com.aerospike.client.Key;

import java.util.Map;

public interface IAerospikeDocumentClient {

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

    /**
     * Put a document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonObject  A JSON object to put.
     */
    void put(Key documentKey, Map<?, ?> jsonObject);

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(Key documentKey, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(Key documentKey, String jsonPath)
            throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException;
}
