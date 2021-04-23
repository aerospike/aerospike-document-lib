package com.aerospike.documentAPI;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

/**
 * Constants used in testing
 */
public class TestConstants {
    public static final String AEROSPIKE_SERVER_IP = "172.28.128.6";
    public static final String AEROSPIKE_NAMESPACE = "test";
    public static final String AEROSPIKE_SET = "documentAPI";

    public static final String JSON_EXAMPLE_KEY = "jsonExampleKey";
    public static final String JSON_EXAMPLE_BIN = "jsonExampleBin";

    public static final Key TEST_AEROSPIKE_KEY = new Key(AEROSPIKE_NAMESPACE,AEROSPIKE_SET,JSON_EXAMPLE_KEY);
    public static final AerospikeClient TEST_AEROSPIKE_CLIENT = new AerospikeClient(AEROSPIKE_SERVER_IP,Constants.AEROSPIKE_SERVER_PORT);

}
