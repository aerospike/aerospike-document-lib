package com.aerospike.documentapi;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * Constants used in testing
 */
public class BaseTestConfig {
    public static final String AEROSPIKE_SERVER_IP = "localhost";
    public static final int AEROSPIKE_SERVER_PORT = 3000;
    public static final String AEROSPIKE_NAMESPACE = "test";
    public static final String AEROSPIKE_SET = "documentAPI";

    public static final String JSON_EXAMPLE_KEY = "jsonExampleKey";
    public static final String JSON_EXAMPLE_BIN = "jsonExampleBin";

    public static final Key TEST_AEROSPIKE_KEY = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY);

    public static IAerospikeClient client;

    @BeforeAll
    public static void setupClass() {
        client = new AerospikeClient(AEROSPIKE_SERVER_IP, AEROSPIKE_SERVER_PORT);
    }

    @AfterAll
    public static void cleanupClass() {
        if (client != null) {
            client.close();
        }
    }
}
