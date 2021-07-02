package com.aerospike.documentapi;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

public interface IAerospikeDocumentClient {

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @param documentBinName The bin name that will store the json.
     * @return Object referenced by jsonPath.
     */
    Object get(Key documentKey, String jsonPath, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @param documentBinName The bin name that will store the json.
     * @return Object referenced by jsonPath.
     */
    Object get(Policy readPolicy, Key documentKey, String jsonPath, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonObject  A JSON object to put.
     * @param documentBinName The bin name that will store the json.
     */
    void put(Key documentKey, JsonNode jsonObject, String documentBinName);

    /**
     * Put a document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonObject  A JSON object to put.
     * @param documentBinName The bin name that will store the json.
     */
    void put(WritePolicy writePolicy, Key documentKey, JsonNode jsonObject, String documentBinName);

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     * @param documentBinName The bin name that will store the json.
     */
    void put(Key documentKey, String jsonPath, Object jsonObject, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     * @param documentBinName The bin name that will store the json.
     */
    void put(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     * @param documentBinName The bin name that will store the json.
     */
    void append(Key documentKey, String jsonPath, Object jsonObject, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     * @param documentBinName The bin name that will store the json.
     */
    void append(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     * @param documentBinName The bin name that will store the json.
     */
    void delete(Key documentKey, String jsonPath, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     * @param documentBinName The bin name that will store the json.
     */
    void delete(WritePolicy writePolicy, Key documentKey, String jsonPath, String documentBinName)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;
}
