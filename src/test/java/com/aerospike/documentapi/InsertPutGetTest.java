package com.aerospike.documentapi;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aerospike.documentapi.BaseTestConfig.AEROSPIKE_SERVER_IP;
import static com.aerospike.documentapi.BaseTestConfig.AEROSPIKE_SERVER_PORT;

public class InsertPutGetTest {

    @org.junit.jupiter.api.Test
    void test() {
        AerospikeClient client = new AerospikeClient(null, AEROSPIKE_SERVER_IP, AEROSPIKE_SERVER_PORT);
        Key key = new Key("test", "customer1", 1);

        Map<String, Object> address1 = new HashMap<>();
        address1.put("street", "123 Main St");
        address1.put("city", "Denver");
        address1.put("zip", "80014");

        Map<String, Object> address2 = new HashMap<>();
        address2.put("street", "222 Smith St");
        address2.put("city", "Atlanta");
        address2.put("zip", "30033");

        List<?> addresses = Arrays.asList(address1, address2);
        client.put(null, key,
            new Bin("name", "Joe"),
            new Bin("age", 28),
            new Bin("addresses", addresses)
        );

        AerospikeDocumentClient docClient = new AerospikeDocumentClient(client);
        docClient.put(key, "addresses", "$.[1].zip", "80015");
        Object docResult = docClient.get(key, "addresses", "$.[*].zip");
        System.out.println(docResult);
    }
}
