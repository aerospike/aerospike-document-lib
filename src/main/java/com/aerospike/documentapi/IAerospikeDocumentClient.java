package com.aerospike.documentapi;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public interface IAerospikeDocumentClient {

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Policy readPolicy, Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Policy readPolicy, Key documentKey, List<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Key documentKey, List<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a document.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonObject  A JSON object to put.
     */
    void put(Key documentKey, String documentBinName, JsonNode jsonObject);

    /**
     * Put a document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonObject  A JSON object to put.
     */
    void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonObject);

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(Key documentKey, List<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    void put(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(Key documentKey, List<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    void append(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(Key documentKey, List<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names that each contains the same structure of a document.
     * @param jsonPath    A JSON path for the object deletion.
     */
    void delete(WritePolicy writePolicy, Key documentKey, List<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;
}
