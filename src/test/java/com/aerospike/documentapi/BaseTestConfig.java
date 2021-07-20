package com.aerospike.documentapi;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

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
    public static String documentBinName = "documentBin";

    public static String events1;
    public static String events2;
    public static String testMaterialJson;
    public static String storeJson;
    public static String tommyLeeJonesJson;
    public static String topLevelArrayTypeJson;

    @BeforeAll
    public static void setupClass() throws IOException {
        client = new AerospikeClient(AEROSPIKE_SERVER_IP, AEROSPIKE_SERVER_PORT);
        loadJsonFiles();
    }

    @AfterAll
    public static void cleanupClass() {
        if (client != null) {
            client.close();
        }
    }

    private static void loadJsonFiles() throws IOException {
        events1 = DebugUtils.readJSONFromAFile("src/test/resources/events1.json");
        events2 = DebugUtils.readJSONFromAFile("src/test/resources/events2.json");
        testMaterialJson = DebugUtils.readJSONFromAFile("src/test/resources/jsonTestMaterial.json");
        storeJson = DebugUtils.readJSONFromAFile("src/test/resources/store.json");
        tommyLeeJonesJson = DebugUtils.readJSONFromAFile("src/test/resources/tommy-lee-jones.json");
        topLevelArrayTypeJson = DebugUtils.readJSONFromAFile("src/test/resources/topLevelArrayType.json");
    }
}
