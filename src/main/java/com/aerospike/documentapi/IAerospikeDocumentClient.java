package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IAerospikeDocumentClient {

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy      An Aerospike read policy to use for the get() operation.
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    Object get(Policy readPolicy, Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path to get the reference from.
     * @return A Map of Objects referenced by jsonPath where a key is a bin name.
     */
    Map<String, Object> get(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a document.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonObject      A JSON object to put.
     */
    void put(Key documentKey, String documentBinName, JsonNode jsonObject);

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to put the given JSON object in.
     * @param jsonObject      A JSON object to put in the given JSON path.
     */
    void put(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path to put the given JSON object in.
     * @param jsonObject       A JSON object to put in the given JSON path.
     */
    void put(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to A collection in a document specified by a JSON path.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path that includes A collection to append the given JSON object to.
     * @param jsonObject      A JSON object to append to the list at the given JSON path.
     */
    void append(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Append an object to A collection in a document specified by a JSON path.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path that includes A collection to append the given JSON object to.
     * @param jsonObject       A JSON object to append to the list at the given JSON path.
     */
    void append(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path for the object deletion.
     */
    void delete(Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path for the object deletion.
     */
    void delete(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;

    /**
     * Perform batch operations.
     * <p>Operations order is preserved only for the operations with different keys.</p>
     * <p>The order of operations with the same keys is not guaranteed, some of them might get omitted.</p>
     * <p>Two-step operations with the same keys are not allowed in a batch.</p>
     *
     * @param batchOperations A list of batch operations to apply.
     * @param parallel        Whether batch processing stream operations should run in parallel.
     * @return The list of corresponding {@link BatchRecord} results.
     */
    List<BatchRecord> batchPerform(List<BatchOperation> batchOperations, boolean parallel)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException;
}
