package com.aerospike.documentapi;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        return get(null, documentKey, documentBinName, jsonPath);
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy      An Aerospike read policy to use for the get() operation.
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to get the reference from.
     * @return Object referenced by jsonPath.
     */
    public Object get(Policy readPolicy, Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Object result = aerospikeDocumentRepository.get(readPolicy, documentKey, documentBinName, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            return JsonPathQuery.read(jsonPathObject, result);
        } else {
            return result;
        }
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param readPolicy       An Aerospike read policy to use for the get() operation.
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path to get the reference from.
     * @return A Map of Objects referenced by jsonPath where a key is a bin name.
     */
    public Map<String, Object> get(Policy readPolicy, Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);

        Map<String, Object> result = aerospikeDocumentRepository.get(readPolicy, documentKey, documentBinNames, jsonPathObject);
        if (jsonPathObject.requiresJsonPathQuery()) {
            Map<String, Object> results = new HashMap<>();
            for (String binName : result.keySet()) {
                results.put(binName, JsonPathQuery.read(jsonPathObject, result.get(binName)));
            }
            return results;
        } else {
            return result;
        }
    }

    /**
     * Retrieve the object in the document with key documentKey that is referenced by the JSON path.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path to get the reference from.
     * @return A Map of Objects referenced by jsonPath where a key is a bin name.
     */
    public Map<String, Object> get(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        return get(null, documentKey, documentBinNames, jsonPath);
    }

    /**
     * Put a document.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonNode        A JSON node to put.
     */
    public void put(Key documentKey, String documentBinName, JsonNode jsonNode) {
        put(null, documentKey, documentBinName, jsonNode);
    }

    /**
     * Put a document.
     *
     * @param writePolicy     An Aerospike write policy to use for the put() operation.
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonNode        A JSON node to put.
     */
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode) {
        aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, jsonNode);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to put the given JSON object in.
     * @param jsonObject      A JSON object to put in the given JSON path.
     */
    public void put(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        put(null, documentKey, documentBinName, jsonPath, jsonObject);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy     An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path to put the given JSON object in.
     * @param jsonObject      A JSON object to put in the given JSON path.
     */
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
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
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path to put the given JSON object in.
     * @param jsonObject       A JSON object to put in the given JSON path.
     */
    public void put(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        put(null, documentKey, documentBinNames, jsonPath, jsonObject);
    }

    /**
     * Put a map representation of a JSON object at a particular path in a JSON document.
     *
     * @param writePolicy      An Aerospike write policy to use for the put() and operate() operations.
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path to put the given JSON object in.
     * @param jsonObject       A JSON object to put in the given JSON path.
     */
    public void put(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinNames, jsonPathObject);
            Map<String, Object> queryResults = new HashMap<>();
            for (String binName : result.keySet()) {
                queryResults.put(binName, JsonPathQuery.set(jsonPathObject, result.get(binName), jsonObject));
            }
            aerospikeDocumentRepository.put(writePolicy, documentKey, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinNames, jsonObject, jsonPathObject);
        }
    }

    /**
     * Append an object to A collection in a document specified by a JSON path.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path that includes A collection to append the given JSON object to.
     * @param jsonObject      A JSON object to append to the list at the given JSON path.
     */
    public void append(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        append(null, documentKey, documentBinName, jsonPath, jsonObject);
    }

    /**
     * Append an object to A collection in a document specified by a JSON path.
     *
     * @param writePolicy     An Aerospike write policy to use for the operate() operation.
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path that includes A collection to append the given JSON object to.
     * @param jsonObject      A JSON object to append to the list at the given JSON path.
     */
    public void append(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
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
     * Append an object to A collection in a document specified by a JSON path.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path that includes A collection to append the given JSON object to.
     * @param jsonObject       A JSON object to append to the list at the given JSON path.
     */
    public void append(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        append(null, documentKey, documentBinNames, jsonPath, jsonObject);
    }

    /**
     * Append an object to A collection in a document specified by a JSON path.
     *
     * @param writePolicy      An Aerospike write policy to use for the operate() operation.
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path that includes A collection to append the given JSON object to.
     * @param jsonObject       A JSON object to append to the list at the given JSON path.
     */
    public void append(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinNames, jsonPathObject);
            Map<String, Object> queryResults = new HashMap<>();
            for (String binName : result.keySet()) {
                queryResults.put(binName, JsonPathQuery.append(jsonPathObject, result.get(binName), jsonObject));
            }
            aerospikeDocumentRepository.put(writePolicy, documentKey, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.append(writePolicy, documentKey, documentBinNames, jsonPath, jsonObject, jsonPathObject);
        }
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path for the object deletion.
     */
    public void delete(Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        delete(null, documentKey, documentBinName, jsonPath);
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy     An Aerospike write policy to use for the operate() operation.
     * @param documentKey     An Aerospike Key.
     * @param documentBinName The bin name that will store the json.
     * @param jsonPath        A JSON path for the object deletion.
     */
    public void delete(WritePolicy writePolicy, Key documentKey, String documentBinName, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
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

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path for the object deletion.
     */
    public void delete(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        delete(null, documentKey, documentBinNames, jsonPath);
    }

    /**
     * Delete an object in a document specified by a JSON path.
     *
     * @param writePolicy      An Aerospike write policy to use for the operate() operation.
     * @param documentKey      An Aerospike Key.
     * @param documentBinNames A collection of bin names that each contains the same structure of a document.
     * @param jsonPath         A JSON path for the object deletion.
     */
    public void delete(WritePolicy writePolicy, Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        if (jsonPathObject.requiresJsonPathQuery()) {
            JsonPathObject originalJsonPathObject = jsonPathObject.copy();
            Map<String, Object> result = aerospikeDocumentRepository.get(writePolicy, documentKey, documentBinNames, jsonPathObject);
            Map<String, Object> queryResults = new HashMap<>();
            for (String binName : result.keySet()) {
                queryResults.put(binName, JsonPathQuery.delete(jsonPathObject, result.get(binName)));
            }
            aerospikeDocumentRepository.put(writePolicy, documentKey, queryResults, originalJsonPathObject);
        } else {
            aerospikeDocumentRepository.delete(writePolicy, documentKey, documentBinNames, jsonPath, jsonPathObject);
        }
    }
}
