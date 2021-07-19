package com.aerospike.documentapi;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * Primary object for accessing and mutating documents.
 */
public class AerospikeDocumentClient implements IAerospikeDocumentClient {
    private final AerospikeDocumentRepository aerospikeDocumentRepository;

    public AerospikeDocumentClient(IAerospikeClient client) {
        this.aerospikeDocumentRepository = new AerospikeDocumentRepository(client);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        return get(null, documentKey, documentBinName, jsonPath);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Policy readPolicy, Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Object result = aerospikeDocumentRepository.get(readPolicy, documentKey, documentBinName, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            return JsonPathQuery.read(jsonPathObject, result);
        }
        else {
            return result;
        }
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, List<String> documentBinNames, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        return get(null, documentKey, documentBinNames, jsonPath);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy  An Aerospike read policy to use for the get() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinNames A list of bin names
     * @param jsonPath    A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Policy readPolicy, Key documentKey, List<String> documentBinNames, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Object result = aerospikeDocumentRepository.get(readPolicy, documentKey, documentBinNames, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            return JsonPathQuery.read(jsonPathObject, result);
        }
        else {
            return result;
        }
    }

    /**
     * Put a document.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonNode  A JSON node to put.
     */
    public void put(Key documentKey, String documentBinName, JsonNode jsonNode) {
        put(null, documentKey, documentBinName, jsonNode);
    }

    /**
     * Put a document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonNode  A JSON node to put.
     */
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode) {
        aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, jsonNode);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    public void put(Key documentKey, String documentBinName, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        put(null, documentKey, documentBinName, jsonPath, jsonObject);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path to put the given JSON object in.
     * @param jsonObject  A JSON object to put in the given JSON path.
     */
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Object result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinName, jsonPathObject);
            Object queryResult = JsonPathQuery.set(jsonPathObject, result, jsonObject);
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, queryResult, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, jsonObject, jsonPathObject);
        }
    }

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    public void append(Key documentKey, String documentBinName, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        append(null, documentKey, documentBinName, jsonPath, jsonObject);
    }

    /**
     * Append an object to a list in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path that includes a list to append the given JSON object to.
     * @param jsonObject  A JSON object to append to the list at the given JSON path.
     */
    public void append(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Object result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinName, jsonPathObject);
            Object queryResult = JsonPathQuery.append(jsonPathObject, result, jsonObject);
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, queryResult, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.append(writePolicy, documentKey, documentBinName, jsonPath, jsonObject, jsonPathObject);
        }
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path for the object deletion.
     */
    public void delete(Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        delete(null, documentKey, documentBinName, jsonPath);
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy An Aerospike write policy to use for the operate() operation.
     * @param documentKey An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath    A JSON path for the object deletion.
     */
    public void delete(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Object result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinName, jsonPathObject);
            Object queryResult = JsonPathQuery.delete(jsonPathObject, result);
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, queryResult, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.delete(writePolicy, documentKey, documentBinName, jsonPath, jsonPathObject);
        }
    }
}
