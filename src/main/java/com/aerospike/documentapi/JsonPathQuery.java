package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.pathparts.PathPart;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;

import java.util.Map;

public class JsonPathQuery {

    IAerospikeClient client;

    public JsonPathQuery(IAerospikeClient client) {
        this.client = client;
    }

    public Object execute(Policy readPolicy, JsonPathObject jsonPathObject, Key documentKey, String documentBinName) throws DocumentApiException, JsonProcessingException {
        String json;
        String jsonPath;
        Record r;
        Object fullDocument;

        // If there are no parts, retrieve the full document
        if (jsonPathObject.getPathParts().size() == 0) {
            fullDocument = client.get(readPolicy, documentKey).getValue(documentBinName);
            json = JsonConverters.convertMapToJsonString((Map<?, ?>) fullDocument);
            jsonPath = "$" +
                    jsonPathObject.getJsonPathSecondStepQuery();
        } else {
            // We need to treat the last part of the path differently
            PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(jsonPathObject.getPathParts());
            // Then turn the rest into the contexts representation
            CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(jsonPathObject.getPathParts());
            // Retrieve the part of the document referred to by the JSON path

            try {
                WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
                r = client.operate(writePolicy, documentKey,
                        finalPathPart.toAerospikeGetOperation(documentBinName, ctxArray));
                json = JsonConverters.convertMapToJsonString(r.bins);
                jsonPath = "$." +
                        documentBinName +
                        jsonPathObject.getJsonPathSecondStepQuery();
            } catch (AerospikeException e) {
                throw DocumentApiException.toDocumentException(e);
            }
        }

        return JsonPath.read(json, jsonPath);
    }
}
