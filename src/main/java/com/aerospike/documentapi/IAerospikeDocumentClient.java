package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IAerospikeDocumentClient {

    /**
     * Retrieve the object in the document with the key that is referenced by JSON path.
     *
     * @param key      An Aerospike Key.
     * @param binName  The bin name that will store the json.
     * @param jsonPath A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    Object get(Key key, String binName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Retrieve the object in the document with the key that is referenced by JSON path.
     *
     * @param key      An Aerospike Key.
     * @param binNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath A JSON path to get the reference from.
     * @return A map of objects referenced by jsonPath where bin name is a key.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    Map<String, Object> get(Key key, Collection<String> binNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Put a document.
     *
     * @param key        An Aerospike Key.
     * @param binName    The bin name that will store the json.
     * @param jsonObject A JSON object to put.
     */
    void put(Key key, String binName, JsonNode jsonObject);

    /**
     * Put a map representation of a JSON object at a particular path in JSON document.
     *
     * @param key        An Aerospike Key.
     * @param binName    The bin name that will store the json.
     * @param jsonPath   A JSON path to put the given JSON object in.
     * @param jsonObject A JSON object to put in the given JSON path.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    void put(Key key, String binName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Put a map representation of a JSON object at a particular path in JSON document.
     *
     * @param key        An Aerospike Key.
     * @param binNames   A collection of bin names that each contains the same structure of a document.
     * @param jsonPath   A JSON path to put the given JSON object in.
     * @param jsonObject A JSON object to put in the given JSON path.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    void put(Key key, Collection<String> binNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Append an object to a collection in a document specified by JSON path.
     *
     * @param key        An Aerospike Key.
     * @param binName    The bin name that will store the json.
     * @param jsonPath   A JSON path that includes a collection to append the given JSON object to.
     * @param jsonObject A JSON object to append to the list at the given JSON path.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    void append(Key key, String binName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Append an object to a collection in a document specified by JSON path.
     *
     * @param key        An Aerospike Key.
     * @param binNames   A collection of bin names that each contains the same structure of a document.
     * @param jsonPath   A JSON path that includes a collection to append the given JSON object to.
     * @param jsonObject A JSON object to append to the list at the given JSON path.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    void append(Key key, Collection<String> binNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Delete an object in a document specified by JSON path.
     *
     * @param key      An Aerospike Key.
     * @param binName  The bin name that will store the json.
     * @param jsonPath A JSON path for the object deletion.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    void delete(Key key, String binName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Delete an object in a document specified by JSON path.
     *
     * @param key      An Aerospike Key.
     * @param binNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath A JSON path for the object deletion.
     * @throws DocumentApiException              if there was an error while executing or post-production.
     * @throws JsonPathParser.JsonParseException if there was an error during JSON parsing.
     */
    void delete(Key key, Collection<String> binNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException;

    /**
     * Perform batch operations.
     * <p>Operations order is preserved only for the operations with different keys.</p>
     * <p>The order and consistency of one-step (JSON path) operations with the same keys is not guaranteed.</p>
     * <p>Two-step (JSONPath query) operations with the same keys are not allowed in a batch.</p>
     *
     * @param batchOperations A list of batch operations to apply.
     * @param parallel        Whether batch processing stream operations should run in parallel.
     * @return The list of corresponding {@link BatchRecord} results.
     * @throws IllegalArgumentException if the batch has multiple two-step operations with the same key.
     */
    List<BatchRecord> batchPerform(List<BatchOperation> batchOperations, boolean parallel)
            throws JsonPathParser.JsonParseException, DocumentApiException;
}