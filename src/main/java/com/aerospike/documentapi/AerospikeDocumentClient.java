package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Primary object for accessing and mutating documents.
 */
public class AerospikeDocumentClient implements IAerospikeDocumentClient {
    public static String DEFAULT_DOCUMENT_BIN_NAME = "documentBin";

    private final IAerospikeClient client;
    private AerospikeDocumentEngine aerospikeDocumentEngine;
    private String documentBinName = DEFAULT_DOCUMENT_BIN_NAME;

    public AerospikeDocumentClient(IAerospikeClient client) {
        this.client = client;
        this.aerospikeDocumentEngine = new AerospikeDocumentEngine(client, documentBinName);
    }

    public AerospikeDocumentClient(IAerospikeClient client, String documentBinName) {
        this(client);
        this.documentBinName = documentBinName;
        this.aerospikeDocumentEngine = new AerospikeDocumentEngine(client, documentBinName);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        return get(null, documentKey, jsonPath);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Policy readPolicy, Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Object result = aerospikeDocumentEngine.get(readPolicy, documentKey, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            return JsonPathQuery.execute(jsonPathObject, result);
        }
        else {
            return result;
        }
    }

    /**
     * Put a document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonNode  A JSON node to put.
     */
    public void put(Key documentKey, JsonNode jsonNode) {
        put(null, documentKey, jsonNode);
    }

    /**
     * Put a document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonNode  A JSON node to put.
     */
    public void put(WritePolicy writePolicy, Key documentKey, JsonNode jsonNode) {
        client.put(writePolicy, documentKey, Utils.createBinByJsonNodeType(documentBinName, jsonNode));
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    public void put(Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException {
        put(null, documentKey, jsonPath, jsonObject);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    public void put(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            throw new DocumentApiException.QueryToANonReadOperationException(new AerospikeException("Query to a non-read operation exception."));
        } else {
            aerospikeDocumentEngine.put(writePolicy, documentKey, jsonObject, jsonPathObject);
        }
    }

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    public void append(Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException {
        append(null, documentKey, jsonPath, jsonObject);
    }

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    public void append(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            throw new DocumentApiException.QueryToANonReadOperationException(new AerospikeException("Query to a non-read operation exception."));
        } else {
            aerospikeDocumentEngine.append(writePolicy, documentKey, jsonPath, jsonObject, jsonPathObject);
        }
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     */
    public void delete(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException {
        delete(null, documentKey, jsonPath);
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path for the object deletion.
     */
    public void delete(WritePolicy writePolicy, Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            throw new DocumentApiException.QueryToANonReadOperationException(new AerospikeException("Query to a non-read operation exception."));
        } else {
            aerospikeDocumentEngine.delete(writePolicy, documentKey, jsonPath, jsonPathObject);
        }
    }
}
