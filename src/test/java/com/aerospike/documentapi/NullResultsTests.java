package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.Policy;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.policy.DocumentPolicy;
import com.aerospike.documentapi.util.JsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

public class NullResultsTests extends BaseTestConfig {

    @Test
    public void testNullResultsWithFilterExpression() throws DocumentApiException {
        Policy readPolicy = client.getReadPolicyDefault();
        readPolicy.filterExp = Exp.build(Exp.eq(Exp.stringBin("docBin"), Exp.val("hi")));
        readPolicy.failOnFilteredOut = false;

        DocumentPolicy documentPolicy = DocumentPolicy.builder().readPolicy(readPolicy).build();
        AerospikeDocumentClient docClient = new AerospikeDocumentClient(client, documentPolicy);

        String jsonString = "{\"k1\":\"v1\", \"k2\":[1,2,3], \"k3\":[\"v31\", \"v32\", \"v34\"]}";
        //Convert json string to json node
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        //Record Key object
        Key doc1 = new Key("test", "docset", "doc1");
        //Insert document to DB
        docClient.put(doc1, "docBin", jsonNode);

        Map<String, List<Integer>> a = new HashMap<>();

        List<Integer> jsonStringList = new Vector<>();
        jsonStringList.add(1);
        jsonStringList.add(2);
        jsonStringList.add(3);
        jsonStringList.add(4);
        jsonStringList.add(5);

        a.put("x1", jsonStringList);

        docClient.put(doc1, "docBin", "$.k4", a);

        Object result = docClient.get(doc1, "docBin", "$.k4");
        assertNull(result);

        readPolicy.failOnFilteredOut = true;
        try {
            docClient.get(doc1, "docBin", "$.k4");
            fail("Should fail with AerospikeException: Transaction filtered out.");
        } catch (AerospikeException ignored) {
        }
    }
}