package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Primary object for accessing and mutating documents.
 */
public class AerospikeDocumentClient implements IAerospikeDocumentClient {
    public static String DEFAULT_DOCUMENT_BIN_NAME = "documentBin";

    private final IAerospikeClient client;
    private String documentBinName = DEFAULT_DOCUMENT_BIN_NAME;

    public AerospikeDocumentClient(IAerospikeClient client) {
        this.client = client;
    }

    public AerospikeDocumentClient(IAerospikeClient client, String documentBinName) {
        this(client);
        this.documentBinName = documentBinName;
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException {
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
            DocumentApiException {
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, retrieve the full document
        if (pathParts.size() == 0) {
            return client.get(readPolicy, documentKey).getValue(documentBinName);
        }
        // else retrieve using contexts
        else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.remove(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            // Retrieve the part of the document referred to by the JSON path
            Record r;
            try {
                WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
                r = client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeGetOperation(documentBinName, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
            return r.getValue(documentBinName);
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
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, put the full document
        if (pathParts.size() == 0) {
            client.put(writePolicy, documentKey, new Bin(documentBinName, jsonObject));
        }
        // else put using contexts
        else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.remove(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikePutOperation(documentBinName, jsonObject, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
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
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, you can't append
        if (pathParts.size() == 0) {
            throw new JsonPathParser.ListException(jsonPath);
        } else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.get(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeAppendOperation(documentBinName, jsonObject, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
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
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, you can't append
        if (pathParts.size() == 0) {
            throw new JsonPathParser.ListException(jsonPath);
        } else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.remove(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeDeleteOperation(documentBinName, ctxArray));
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }
    }
}
