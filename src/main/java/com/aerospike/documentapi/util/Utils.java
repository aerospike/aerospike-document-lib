package com.aerospike.documentapi.util;

import com.aerospike.client.Bin;
import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.PathDetails;
import com.aerospike.documentapi.jsonpath.pathpart.PathPart;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.experimental.UtilityClass;

import java.util.List;

@UtilityClass
public class Utils {

    public static Bin createBinByJsonNodeType(String binName, JsonNode jsonNode) {
        if (jsonNode.isArray()) {
            return new Bin(binName, JsonConverters.convertJsonNodeToList(jsonNode));
        } else {
            return new Bin(binName, JsonConverters.convertJsonNodeToMap(jsonNode));
        }
    }

    public static void validateNotArray(Object object) throws IllegalArgumentException {
        if (object.getClass().isArray()) {
            throw new IllegalArgumentException("Putting/appending an array is not allowed, consider providing a Collection");
        }
    }

    public static PathDetails getPathDetails(List<PathPart> pathParts, boolean modify) {
        // We need to treat the last part of the path differently and without modifying
        PathPart finalPathPart = modify ? JsonPathParser.extractLastPathPartAndModifyList(pathParts)
                : JsonPathParser.extractLastPathPart(pathParts);
        // Then turn the rest into the contexts representation
        CTX[] ctxArray = JsonPathParser.pathPartsToContextArray(pathParts);

        return new PathDetails(finalPathPart, ctxArray);
    }
}