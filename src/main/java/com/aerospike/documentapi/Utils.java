package com.aerospike.documentapi;

import com.aerospike.client.Bin;
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
}
