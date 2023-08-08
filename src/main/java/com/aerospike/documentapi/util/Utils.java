package com.aerospike.documentapi.util;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.jsonpath.PathDetails;
import com.aerospike.documentapi.token.ContextAwareToken;
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

    public static Bin createBin(String binName, Object jsonObject) {
        return new Bin(binName, Value.get(jsonObject));
    }

    public static void validateNotArray(Object object) throws IllegalArgumentException {
        if (object.getClass().isArray()) {
            throw new IllegalArgumentException("Putting/appending an array is not allowed, consider providing a " +
                    "Collection");
        }
    }

    public static PathDetails getPathDetails(List<ContextAwareToken> tokens, boolean modify) {
        // We need to treat the last part of the path differently and without modifying
        ContextAwareToken finalToken = modify ? JsonPathParser.extractLastPathPartAndModifyList(tokens)
                : JsonPathParser.extractLastPathPart(tokens);
        // Then turn the rest into the contexts representation
        CTX[] ctxArray = JsonPathParser.pathTokensToContextArray(tokens);

        return new PathDetails(finalToken, ctxArray);
    }

    public static boolean isBlank(String string) {
        return string == null || string.trim().isEmpty();
    }

    public static JsonPathObject validateJsonPathSingleStep(JsonPathObject jsonPathObject, String msg) {
        if (jsonPathObject.requiresJsonPathQuery()) {
            throw new IllegalArgumentException(msg);
        }
        return jsonPathObject;
    }
}
