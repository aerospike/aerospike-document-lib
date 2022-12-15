package com.aerospike.documentapi.util;

import com.aerospike.client.Key;
import com.aerospike.documentapi.IAerospikeDocumentClient;
import com.aerospike.documentapi.IAerospikeDocumentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
public class TestUtils {

    public void writeDocumentToDB(Key key, String binName, Object json, IAerospikeDocumentClient documentClient,
                                  IAerospikeDocumentRepository repository) {
        if (json instanceof JsonNode) {
            documentClient.put(key, binName, (JsonNode) json);
        } else if (json instanceof Map) {
            repository.put(null, key, binName, (Map<?, ?>) json);
        }
    }
}