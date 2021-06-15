package com.aerospike.documentapi;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

import java.util.List;
import java.util.Map;

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

    /**
     * The bin used to store the documents is configurable.
     */
    public String getDocumentBinName() {
        return documentBinName;
    }

    /**
     * Set the bin used to store documents.
     */
    public void setDocumentBinName(String documentBinName) {
        this.documentBinName = documentBinName;
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
        return get(client.getReadPolicyDefault(), client.getWritePolicyDefault(), documentKey, jsonPath);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     * @param readPolicy An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Policy readPolicy, Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
        return get(readPolicy, client.getWritePolicyDefault(), documentKey, jsonPath);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     * @param readPolicy An Aerospike read policy to use for the get() operation.
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Policy readPolicy, WritePolicy writePolicy, Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
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
                r = client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeGetOperation(documentBinName, ctxArray));
            } catch (AerospikeException e) {
                throw AerospikeDocumentClientExceptions.toDocumentException(e);
            }
            return r.getValue(documentBinName);
        }
    }

    /**
     * Put a document.
     * @param documentKey An Aerospike Key.
     * @param jsonObject A JSON object to put.
     */
    public void put(Key documentKey, Map<?, ?> jsonObject) {
        put(client.getWritePolicyDefault(), documentKey, jsonObject);
    }

    /**
     * Put a document.
     * @param writePolicy An Aerospike write policy to use for the put() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonObject A JSON object to put.
     */
    public void put(WritePolicy writePolicy, Key documentKey, Map<?, ?> jsonObject) {
        client.put(writePolicy, documentKey, new Bin(documentBinName, jsonObject));
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to put the given JSON object in.
     * @param jsonObject A JSON object to put in the given JSON path.
     */
    public void put(Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
        put(client.getWritePolicyDefault(), documentKey, jsonPath, jsonObject);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to put the given JSON object in.
     * @param jsonObject A JSON object to put in the given JSON path.
     */
    public void put(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
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
                throw AerospikeDocumentClientExceptions.toDocumentException(e);
            }
        }
    }

    /**
     * Append an object to a list in a document specified by a JSON path.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject A JSON object to append to the list at the given JSON path.
     */
    public void append(Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
        append(client.getWritePolicyDefault(), documentKey, jsonPath, jsonObject);
    }

    /**
     * Append an object to a list in a document specified by a JSON path.
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject A JSON object to append to the list at the given JSON path.
     */
    public void append(WritePolicy writePolicy, Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
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
                throw AerospikeDocumentClientExceptions.toDocumentException(e);
            }
        }
    }

    /**
     * Delete an object in a document specified by a JSON path.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path for the object deletion.
     */
    public void delete(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
        delete(client.getWritePolicyDefault(), documentKey, jsonPath);
    }

    /**
     * Delete an object in a document specified by a JSON path.
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path for the object deletion.
     */
    public void delete(WritePolicy writePolicy, Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientExceptions.AerospikeDocumentClientException {
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
                throw AerospikeDocumentClientExceptions.toDocumentException(e);
            }
        }
    }
}
