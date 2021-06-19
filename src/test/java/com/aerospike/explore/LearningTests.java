package com.aerospike.explore;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentapi.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * These are not really tests - just code I've written to work out how to do various things
 */
public class LearningTests extends BaseTestConfig {

    /**
     * Read json from file & output to console
     */
    @Test
    public void getJsonFromFile() throws IOException {
        String s = DebugUtils.readJSONFromAFile("src/test/resources/tommy-lee-jones.json");
        System.out.println(s);
    }

    /**
     * Turn POJO into a string.
     */
    @Test
    public void pojoToString() throws JsonProcessingException {// Java objects to JSON string - compact-print
        String jsonString = pojoToString(getTestStaffMemberObject());
        DebugUtils.consoleHeader("Object mapper POJO -> String");
        System.out.println(jsonString);
        DebugUtils.newLine();
    }

    /**
     * POJO as pretty printed string.
     */
    @Test
    public void pojoToStringPrettyPrint() throws JsonProcessingException {
        String jsonString = pojoToPrettyString(getTestStaffMemberObject());

        DebugUtils.consoleHeader("Object mapper POJO -> String");
        System.out.println(jsonString);
        DebugUtils.newLine();
    }

    /**
     * JSON string to map.
     */
    @Test
    public void stringToMap() throws IOException{
        JsonNode result = JsonConverters.convertStringToJsonNode(pojoToString(getTestStaffMemberObject()));
        DebugUtils.consoleHeader("Read JSON string into map- show map.toString");
        System.out.println(result);
        DebugUtils.newLine();
    }

    /**
     * Convert POJO to map, save to Aerospike DB and read.
     */
    @Test
    public void writePOJOToDB() throws IOException{
        JsonNode result = JsonConverters.convertStringToJsonNode(pojoToString(getTestStaffMemberObject()));
        putJsonNodeToDB(result, TEST_AEROSPIKE_KEY);

        Map<?, ?> m = getMapFromDB(TEST_AEROSPIKE_KEY);
        deleteKey(TEST_AEROSPIKE_KEY);

        DebugUtils.consoleHeader("Putting and getting from the db");
        System.out.println(m);
        DebugUtils.newLine();
    }

    @Test
    public void demo() throws IOException, JsonPathParser.JsonParseException, DocumentApiException {
        String jsonString = DebugUtils.readJSONFromAFile("src/test/resources/tommy-lee-jones.json");

        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);

        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(jsonString);
        Key tommyLeeJonesDBKey = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "src/test/resources/tommy-lee-jones.json");
        documentClient.put(tommyLeeJonesDBKey, jsonNode);

        documentClient.put(tommyLeeJonesDBKey, "$.imdb_rank.rank",45);
        List<String> _2019Films = new Vector<>();
        _2019Films.add("Ad Astra");
        documentClient.put(tommyLeeJonesDBKey, "$.selected_filmography.2019",_2019Films);

        documentClient.append(tommyLeeJonesDBKey, "$.best_films_ranked[0].films", "Rolling Thunder");
        documentClient.append(tommyLeeJonesDBKey, "$.best_films_ranked[0].films", "The Three Burials Of Melquiades Estrada");
        documentClient.delete(tommyLeeJonesDBKey, "$.best_films_ranked[1]");

        System.out.println(documentClient.get(tommyLeeJonesDBKey, "$.best_films_ranked[0].films[0]"));
        System.out.println(documentClient.get(tommyLeeJonesDBKey, "$.best_films_ranked[0].films[5]"));
        System.out.println(documentClient.get(tommyLeeJonesDBKey, "$.selected_filmography.2019"));
    }

    private static StaffMember getTestStaffMemberObject() {
        StaffMember staff = new StaffMember();

        staff.setName("mkyong");
        staff.setAge(38);
        staff.setPosition(new String[]{"Founder", "CTO", "Writer"});
        Map<String, BigDecimal> salary = new HashMap<String, BigDecimal>() {{
            put("2010", new BigDecimal(10000));
            put("2012", new BigDecimal(12000));
            put("2018", new BigDecimal(14000));
        }};
        staff.setSalary(salary);
        staff.setSkills(Arrays.asList("java", "python", "node", "kotlin"));

        return staff;
    }

    private static void putJsonNodeToDB(JsonNode jsonNode, Key key) {
        client.put(null, key, Utils.createBinByJsonNodeType(JSON_EXAMPLE_BIN, jsonNode));
    }

    private static Map<?, ?> getMapFromDB(Key key) {
        Record r = client.get(new Policy(), key);
        return r.getMap(JSON_EXAMPLE_BIN);
    }

    private static void deleteKey(Key key) {
        client.delete(getWritePolicy(), key);
    }

    private static WritePolicy getWritePolicy() {
        WritePolicy writePolicy = new WritePolicy();
        //writePolicy.durableDelete = true;
        return writePolicy;
    }

    private static String pojoToString(TestPOJO p) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        // Java objects to JSON string - compact-print
        return mapper.writeValueAsString(p);
    }

    private static String pojoToPrettyString(TestPOJO p) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        // Java objects to JSON string - compact-print
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p);
    }
}
