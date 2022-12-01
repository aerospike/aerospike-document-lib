package com.aerospike.documentapi.utils;

import com.aerospike.client.Bin;
import com.aerospike.documentapi.JsonConverters;
import com.fasterxml.jackson.databind.JsonNode;

public class Utils {

    private Utils() {
    }

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
}
