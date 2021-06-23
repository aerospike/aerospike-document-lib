package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.pathparts.PathPart;
import com.aerospike.documentapi.pathparts.MapPathPart;
import com.aerospike.documentapi.pathparts.PathPartTypeEnum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.JsonPath;

import java.util.List;

public class JsonPathQuery {

    IAerospikeClient client;

    public JsonPathQuery(IAerospikeClient client) {
        this.client = client;
    }

    public Object wildcardInBracketsQuery(Policy readPolicy, JsonPathObject jsonPathObject, Key documentKey, String documentBinName) throws DocumentApiException {
        int wildcardIndex = 0;

        for (int i = 0; i < jsonPathObject.getAccessPathParts().size(); i++) {
            if (jsonPathObject.getAccessPathParts().get(i).getType().equals(PathPartTypeEnum.MAP)) {
                if (((MapPathPart)jsonPathObject.getAccessPathParts().get(i)).isWildcard()) {
                    wildcardIndex = i+1;
                    break;
                }
            }
        }

        List<PathPart> pathPartsTillWildCard = jsonPathObject.getAccessPathParts().subList(0, wildcardIndex);
        PathPart pathPartAfterWildCard = jsonPathObject.getAccessPathParts().get(wildcardIndex);

        // We need to treat the last part of the path differently
        PathPart finalPathPart = JsonPathParser.extractLastPathPartAndModifyList(pathPartsTillWildCard);
        // Then turn the rest into the contexts representation
        CTX[] ctxArray = JsonPathParser.pathPartsToContextsArray(pathPartsTillWildCard);
        // Retrieve the part of the document referred to by the JSON path
        Record r;
        try {
            WritePolicy writePolicy = readPolicy == null ? null : new WritePolicy(readPolicy);
            r = client.operate(writePolicy, documentKey,
                    finalPathPart.toAerospikeGetOperation(documentBinName, ctxArray));
        } catch (AerospikeException e) {
            throw DocumentApiException.toDocumentException(e);
        }

        String json;

        try {
            json = JsonConverters.convertMapToJsonString(r.bins);
        } catch (JsonProcessingException e){
            throw new AerospikeException(e);
        }

        String jsonPath = "$." +
                documentBinName +
                "[*]." +
                ((MapPathPart) pathPartAfterWildCard).getKey();

        return JsonPath.read(json, jsonPath);
    }
}
