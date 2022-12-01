package com.aerospike.documentapi.util;

import com.aerospike.client.Bin;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.experimental.UtilityClass;

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
}
