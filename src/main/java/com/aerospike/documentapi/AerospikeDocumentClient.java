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

    @Override
    public Object get(Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        return get(null, documentKey, documentBinName, jsonPath);
    }

    @Override
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

    @Override
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

    @Override
    public Map<String, Object> get(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        return get(null, documentKey, documentBinNames, jsonPath);
    }

    @Override
    public void put(Key documentKey, String documentBinName, JsonNode jsonNode) {
        put(null, documentKey, documentBinName, jsonNode);
    }

    @Override
    public void put(WritePolicy writePolicy, Key documentKey, String documentBinName, JsonNode jsonNode) {
        aerospikeDocumentRepository.put(writePolicy, documentKey, documentBinName, jsonNode);
    }

    @Override
    public void put(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        put(null, documentKey, documentBinName, jsonPath, jsonObject);
    }

    @Override
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

    @Override
    public void put(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        put(null, documentKey, documentBinNames, jsonPath, jsonObject);
    }

    @Override
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

    @Override
    public void append(Key documentKey, String documentBinName, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        append(null, documentKey, documentBinName, jsonPath, jsonObject);
    }

    @Override
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

    @Override
    public void append(Key documentKey, Collection<String> documentBinNames, String jsonPath, Object jsonObject)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        append(null, documentKey, documentBinNames, jsonPath, jsonObject);
    }

    @Override
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

    @Override
    public void delete(Key documentKey, String documentBinName, String jsonPath) throws JsonPathParser.JsonParseException,
            DocumentApiException, JsonProcessingException {
        delete(null, documentKey, documentBinName, jsonPath);
    }

    @Override
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

    @Override
    public void delete(Key documentKey, Collection<String> documentBinNames, String jsonPath)
            throws JsonPathParser.JsonParseException, DocumentApiException, JsonProcessingException {
        delete(null, documentKey, documentBinNames, jsonPath);
    }

    @Override
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
