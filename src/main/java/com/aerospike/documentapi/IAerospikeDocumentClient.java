package com.aerospike.documentapi;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.Key;
import com.aerospike.documentapi.batch.BatchOperation;
import com.aerospike.documentapi.data.DocumentFilter;
import com.aerospike.documentapi.data.DocumentQueryStatement;
import com.aerospike.documentapi.data.KeyResult;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
     * <p>Operations order is preserved only for the operations with different keys.</p>
     * <p>The order and consistency of one-step (JSON path) operations with the same keys is not guaranteed.</p>
     * <p>Two-step (JSONPath query) operations with the same keys are not allowed in a batch.</p>
     *
     * @param batchOperations a list of batch operations to apply.
     * @param parallel        whether batch processing stream operations should run in parallel.
     * @return a list of corresponding {@link BatchRecord} results.
     * @throws DocumentApiException     if there was an error.
     * @throws IllegalArgumentException if the batch has multiple two-step operations with the same key.
     */
    List<BatchRecord> batchPerform(List<BatchOperation> batchOperations, boolean parallel);

    /**
     * Perform query.
     * <p>Filtering can be done by setting one or more of the following items:</p>
     * <ul>
     * <li>optional secondary index filter (record level),</li>
     * <li>optional document filter expressions (record level),</li>
     * <li>optional bin names (bin level),</li>
     * <li>optional json paths (inner objects less than a bin if necessary).</li>
     * </ul>
     *
     * @param queryStatement            object for building query definition, storing required bin names, json paths
     *                                  and secondary index filter
     * @param documentFilterExpressions filter expressions
     * @return stream of {@link KeyResult} objects
     * @throws DocumentApiException if query fails
     */
    Stream<KeyResult> query(DocumentQueryStatement queryStatement, DocumentFilter... documentFilterExpressions);
}
