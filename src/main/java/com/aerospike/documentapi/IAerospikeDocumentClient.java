package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.documentapi.batch.BatchOperation;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IAerospikeDocumentClient {

    /**
     * Retrieve an object matched by JSON path.
     *
     * @param key      Aerospike Key.
     * @param binName  name of a bin storing json.
     * @param jsonPath JSON path matching the required elements.
     * @return object matched by jsonPath.
     * @throws DocumentApiException if there was an error.
     */
    Object get(Key key, String binName, String jsonPath);

    /**
     * Retrieve a map of objects matched by JSON path.
     *
     * @param key      Aerospike Key.
     * @param binNames names of bins storing json (all bins with the same document structure).
     * @param jsonPath JSON path matching the required elements.
     * @return A map of objects matched by jsonPath with bin names as keys.
     * @throws DocumentApiException if there was an error.
     */
    Map<String, Object> get(Key key, Collection<String> binNames, String jsonPath);

    /**
     * Put a JSON document.
     *
     * @param key        Aerospike Key.
     * @param binName    name of a bin to store json.
     * @param jsonObject JSON object (document) to put.
     */
    void put(Key key, String binName, JsonNode jsonObject);

    /**
     * Put an object at a particular path in JSON document.
     *
     * @param key      Aerospike Key.
     * @param binName  name of a bin storing json.
     * @param jsonPath A JSON path to put the given JSON object in.
     * @param object   An object to put in the given JSON path.
     * @throws DocumentApiException if there was an error.
     */
    void put(Key key, String binName, String jsonPath, Object object);

    /**
     * Put an object at a particular path in JSON document.
     *
     * @param key      Aerospike Key.
     * @param binNames names of bins storing json (all bins with the same document structure).
     * @param jsonPath JSON path to put the given JSON object in.
     * @param object   the object to be put at the given JSON path.
     * @throws DocumentApiException if there was an error.
     */
    void put(Key key, Collection<String> binNames, String jsonPath, Object object);

    /**
     * Append an object to a collection at a particular path in JSON document.
     *
     * @param key      Aerospike Key.
     * @param binName  name of a bin storing json.
     * @param jsonPath JSON path that includes a collection to append the given JSON object to.
     * @param object   the object to be appended at the given JSON path.
     * @throws DocumentApiException if there was an error.
     */
    void append(Key key, String binName, String jsonPath, Object object);

    /**
     * Append an object to a collection at a particular path in JSON document.
     *
     * @param key      Aerospike Key.
     * @param binNames names of bins storing json (all bins with the same document structure).
     * @param jsonPath JSON path that includes a collection to append the given JSON object to.
     * @param object   the object to be appended at the given JSON path.
     * @throws DocumentApiException if there was an error.
     */
    void append(Key key, Collection<String> binNames, String jsonPath, Object object);

    /**
     * Delete an object at a particular path in JSON document.
     *
     * @param key      Aerospike Key.
     * @param binName  name of a bin storing json.
     * @param jsonPath JSON path for the object deletion.
     * @throws DocumentApiException if there was an error.
     */
    void delete(Key key, String binName, String jsonPath);

    /**
     * Delete an object at a particular path in JSON document.
     *
     * @param key      Aerospike Key.
     * @param binNames names of bins storing json (all bins with the same document structure).
     * @param jsonPath JSON path for the object deletion.
     * @throws DocumentApiException if there was an error.
     */
    void delete(Key key, Collection<String> binNames, String jsonPath);

    /**
     * Perform batch operations.
     *
     * <p>Operations order is preserved only for those 1-step operations
     * (with JSONPath that contains only array and/or map elements)
     * that have unique Aerospike keys within a batch.</p>
     * <p>Every 2-step operation (with JSONPath containing wildcards, recursive descent, filters, functions, scripts)
     * should have unique Aerospike key within a batch.
     *
     * @param batchOperations a list of batch operations to apply.
     * @param parallel        whether batch processing stream operations should run in parallel.
     * @return a list of corresponding {@link BatchRecord} results.
     * @throws DocumentApiException     if there was an error.
     * @throws IllegalArgumentException if the batch has multiple two-step operations with the same key.
     */
    List<BatchRecord> batchPerform(List<BatchOperation> batchOperations, boolean parallel);
}
