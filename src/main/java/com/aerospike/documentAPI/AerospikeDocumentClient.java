package com.aerospike.documentAPI;

import com.aerospike.client.*;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Primary object for accessing and mutating documents
 */
public class AerospikeDocumentClient implements IAerospikeDocumentClient{
    public static String DEFAULT_DOCUMENT_BIN_NAME = "documentBin";

    // Member variables
    private AerospikeClient client;
    private String documentBinName = DEFAULT_DOCUMENT_BIN_NAME;
    private WritePolicy writePolicy;
    private Policy readPolicy;

    /**
     * Constructor
     * @param client - AerospikeClient object
     */
    public AerospikeDocumentClient(AerospikeClient client){
        this.client = client;
        this.writePolicy = client.writePolicyDefault;
        this.readPolicy = client.readPolicyDefault;
    }

    /**
     * Given a serialized json object, return it's equivalent representation as a Java map
     * @param jsonString
     * @return
     * @throws IOException
     */
    public static Map jsonStringToMap(String jsonString) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        // Reading string into a map
        return mapper.readValue(jsonString,Map.class);
    }

    /**
     * The bin used to store the documents is configurable
     * @return documentBinName
     */
    public String getDocumentBinName() {
        return documentBinName;
    }

    /**
     * Set the bin used to store documents
     * @param documentBinName
     */
    public void setDocumentBinName(String documentBinName) {
        this.documentBinName = documentBinName;
    }

    /**
     * Get the read policy used by the client
     * @return Aerospike Read Policy
     */
    public Policy getReadPolicy() {
        return readPolicy;
    }

    /**
     * Set the read policy used by the client
     * @param readPolicy
     */
    public void setReadPolicy(Policy readPolicy) {
        this.readPolicy = readPolicy;
    }

    /**
     * Get the write policy used by the client
     * @return WritePolicy
     */
    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    /**
     * Set the write policy used by the client
     * @param writePolicy
     */
    public void setWritePolicy(WritePolicy writePolicy) {
        this.writePolicy = writePolicy;
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the Json path
     * @param documentKey
     * @param jsonPath
     * @return Object referenced by jsonPath
     * @throws JsonPathParser.JsonParseException
     * @throws AerospikeDocumentClientException
     * @throws AerospikeException
     */
    public Object get(Key documentKey,String jsonPath) throws JsonPathParser.JsonParseException, AerospikeDocumentClientException {
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, retrieve the full document
        if(pathParts.size() == 0){
            return client.get(readPolicy,documentKey).getValue(documentBinName);
        }
        // else retrieve using contexts
        else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.remove(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            // Retrieve the part of the document referred to by the JSON path
            Record r = null;
            try {
                r = client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeGetOperation(documentBinName, ctxArray));
            }
            catch(AerospikeException e){
                processDocumentException(e);
            }
            return r.getValue(documentBinName);
        }
    }

    /**
     * Put a document
     * @param documentKey - document key
     * @param jsonObject - document
     */
    public void put(Key documentKey,Map jsonObject) {
        client.put(writePolicy, documentKey, new Bin(documentBinName, jsonObject));
    }

    /**
     * Put a map representation of a JSON object at a particular path in a json document
     *
     * @param documentKey
     * @param jsonPath
     * @param jsonObject
     * @throws JsonPathParser.JsonParseException
     * @throws AerospikeDocumentClientException
     */
    public void put(Key documentKey,String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException, AerospikeDocumentClientException{
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, put the full document
        if(pathParts.size() == 0){
            client.put(writePolicy,documentKey,new Bin(documentBinName,jsonObject));
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
            }
            catch(AerospikeException e){
                processDocumentException(e);
            }
        }
    }

    /**
     * Append an object to a list in a document specified by a json path
     * @param documentKey
     * @param jsonPath
     * @param jsonObject
     * @throws JsonPathParser.JsonParseException
     * @throws AerospikeDocumentClientException
     */
    public void append(Key documentKey,String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException, AerospikeDocumentClientException{
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, you can't append
        if(pathParts.size() == 0){
            throw new JsonPathParser.ListException(jsonPath);
        }
        else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.get(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeAppendOperation(documentBinName, jsonObject, ctxArray));
            }
            catch(AerospikeException e){
                processDocumentException(e);
            }
        }
    }

    /**
     * Delete an object in a document specified by a json path
     * @param documentKey
     * @param jsonPath
     * @throws JsonPathParser.JsonParseException
     * @throws AerospikeDocumentClient.AerospikeDocumentClientException
     */
    public void delete(Key documentKey,String jsonPath) throws JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException{
        // Turn the String path representation into a PathParts representation
        List<JsonPathParser.PathPart> pathParts = new JsonPathParser().parse(jsonPath);
        // If there are no parts, you can't append
        if(pathParts.size() == 0){
            throw new JsonPathParser.ListException(jsonPath);
        }
        else {
            // We need to treat the last part of the path differently
            JsonPathParser.PathPart finalPathPart = pathParts.remove(pathParts.size() - 1);
            // Then turn the rest into the contexts representation
            List<CTX> contexts = JsonPathParser.pathPartsToContexts(pathParts);
            CTX[] ctxArray = contexts.toArray(new CTX[contexts.size()]);
            try {
                client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeDeleteOperation(documentBinName, ctxArray));
            }
            catch(AerospikeException e){
                processDocumentException(e);
            }
        }
    }

    /**
     * Classes used to type the errors that can be returned
     */
    public abstract static class AerospikeDocumentClientException extends Exception
    {
        AerospikeException e;
        public AerospikeDocumentClientException(AerospikeException e){
            this.e = e;
        }
    }

    /**
     * Thrown if a map or list is accessed that doesn't exist. Also if accessing a list element out of existing lisb bounds
     */
    public static class ObjectNotFoundException extends AerospikeDocumentClientException{
        public ObjectNotFoundException(AerospikeException e){
            super(e);
        }
    }

    /**
     * Thrown if accessing a list as if it was a map, or looking for a key in a map that doesn't exist
     */
    public static class KeyNotFoundException extends AerospikeDocumentClientException{
        public KeyNotFoundException(AerospikeException e){
            super(e);
        }
    }

    /**
     * Thrown if accessing a map as if it were a list or looking for a list element in a list that doesn't exist
     */
    public static class NotAListException extends AerospikeDocumentClientException{
        public NotAListException(AerospikeException e){
            super(e);
        }

    }

    /**
     * Utility method to categorise the different sort of exceptions we will encounter
     * @param e
     * @return
     * @throws AerospikeDocumentClientException
     * @throws AerospikeException
     */
    private static AerospikeDocumentClientException processDocumentException(AerospikeException e) throws
    AerospikeDocumentClientException{
        if(e.getResultCode() == ResultCode.PARAMETER_ERROR) throw new KeyNotFoundException(e);
        else if(e.getResultCode() == ResultCode.BIN_TYPE_ERROR) throw new NotAListException(e);
        else if(e.getResultCode() == ResultCode.OP_NOT_APPLICABLE) throw new ObjectNotFoundException(e);
        else throw e;
    }

}


