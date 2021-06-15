package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
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

    // Member variables
    private final IAerospikeClient client;
    private String documentBinName = DEFAULT_DOCUMENT_BIN_NAME;
    private WritePolicy writePolicy;
    private Policy readPolicy;

    public AerospikeDocumentClient(IAerospikeClient client) {
        this.client = client;
        this.writePolicy = client.getWritePolicyDefault();
        this.readPolicy = client.getReadPolicyDefault();
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
     * Get the read policy used by the client.
     */
    public Policy getReadPolicy() {
        return readPolicy;
    }

    /**
     * Set the read policy used by the client.
     */
    public void setReadPolicy(Policy readPolicy) {
        this.readPolicy = readPolicy;
    }

    /**
     * Get the write policy used by the client.
     */
    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    /**
     * Set the write policy used by the client.
     */
    public void setWritePolicy(WritePolicy writePolicy) {
        this.writePolicy = writePolicy;
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientException {
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
                throw toDocumentException(e);
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
        client.put(writePolicy, documentKey, new Bin(documentBinName, jsonObject));
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path to put the given JSON object in.
     * @param jsonObject A JSON object to put in the given JSON path.
     */
    public void put(Key documentKey, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClientException {
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
                throw toDocumentException(e);
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
            AerospikeDocumentClientException {
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
                throw toDocumentException(e);
            }
        }
    }

    /**
     * Delete an object in a document specified by a JSON path.
     * @param documentKey An Aerospike Key.
     * @param jsonPath A JSON path for the object deletion.
     */
    public void delete(Key documentKey, String jsonPath) throws JsonPathParser.JsonParseException,
            AerospikeDocumentClient.AerospikeDocumentClientException {
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
               throw toDocumentException(e);
            }
        }
    }

    /**
     * Classes used to type the errors that can be returned.
     */
    public abstract static class AerospikeDocumentClientException extends Exception {
        AerospikeException e;
        public AerospikeDocumentClientException(AerospikeException e) {
            this.e = e;
        }
    }

    /**
     * Thrown if a map or list is accessed that doesn't exist. Also if accessing a list element out of existing list bounds.
     */
    public static class ObjectNotFoundException extends AerospikeDocumentClientException {
        public ObjectNotFoundException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Thrown if accessing a list as if it was a map, or looking for a key in a map that doesn't exist.
     */
    public static class KeyNotFoundException extends AerospikeDocumentClientException {
        public KeyNotFoundException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Thrown if accessing a map as if it were a list or looking for a list element in a list that doesn't exist.
     */
    public static class NotAListException extends AerospikeDocumentClientException {
        public NotAListException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Utility method to categorise the different sort of exceptions we will encounter.
     * @param e An AerospikeException.
     * @return A more descriptive case-specific exception.
     */
    private static AerospikeDocumentClientException toDocumentException(AerospikeException e) {
        if (e.getResultCode() == ResultCode.PARAMETER_ERROR) {
            return new KeyNotFoundException(e);
        } else if (e.getResultCode() == ResultCode.BIN_TYPE_ERROR) {
            return new NotAListException(e);
        } else if (e.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
            return new ObjectNotFoundException(e);
        } else {
            throw e;
        }
    }
}
