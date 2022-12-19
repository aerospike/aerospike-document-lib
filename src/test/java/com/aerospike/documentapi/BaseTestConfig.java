package com.aerospike.documentapi;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.documentapi.util.DebugUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.Map;

public class BaseTestConfig {

    public static final String AEROSPIKE_SERVER_IP = "localhost";
    public static final int AEROSPIKE_SERVER_PORT = 3000;
    public static final String AEROSPIKE_NAMESPACE = "test";
    public static final String AEROSPIKE_SET = "documentAPI";

    public static final String JSON_EXAMPLE_KEY = "jsonExampleKey";
    public static final String JSON_EXAMPLE_BIN = "jsonExampleBin";
    public static final String DOCUMENT_BIN_NAME = "documentBin";

    public static final Key TEST_AEROSPIKE_KEY = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, JSON_EXAMPLE_KEY);

    public static IAerospikeClient client;
    public static IAerospikeDocumentClient documentClient;
    public static IAerospikeDocumentRepository documentRepository;

    public static String events1;
    public static String events2;
    public static String testMaterialJson;
    public static String storeJson;
    public static String tommyLeeJonesJson;
    public static String topLevelArrayTypeJson;
    public static String cdtJson;

    @BeforeAll
    public static void setupClass() throws IOException {
        client = new AerospikeClient(AEROSPIKE_SERVER_IP, AEROSPIKE_SERVER_PORT);
        documentClient = new AerospikeDocumentClient(client);
        documentRepository = new AerospikeDocumentRepository(client);
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
        cdtJson = DebugUtils.readJSONFromAFile("src/test/resources/cdt.json");
    }

    protected void writeDocumentToDB(Key key, String binName, Object json) {
        if (json instanceof JsonNode) {
            documentClient.put(key, binName, (JsonNode) json);
        } else if (json instanceof Map) {
            documentRepository.put(null, key, binName, (Map<?, ?>) json);
        }
    }
}
