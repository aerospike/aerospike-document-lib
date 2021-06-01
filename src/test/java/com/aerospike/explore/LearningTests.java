package com.aerospike.explore;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.documentAPI.AerospikeDocumentClient;
import com.aerospike.documentAPI.JsonPathParser;
import com.aerospike.documentAPI.TestConstants;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import com.aerospike.documentAPI.DebugUtils;

/**
 * These are not really tests - just code I've written to work out how to do various things
 */
public class LearningTests {
    @Test
    /**
     * Read json from file & output to console
     */
    public void getJsonFromFile() throws IOException {
        String s = DebugUtils.readLineByLineJava("src/main/resources/tommy-lee-jones.json");
        System.out.println(s);

    }

    @Test
    /**
     * Turn POJO into a string
     */
    public void pojoToString() throws JsonProcessingException {// Java objects to JSON string - compact-print
        String jsonString = pojoToString(getTestStaffMemberObject());
        DebugUtils.consoleHeader("Object mapper POJO -> String");
        System.out.println(jsonString);
        DebugUtils.newLine();
    }

    @Test
    /**
     * POJO as pretty printed string
     */
    public void pojoToStringPrettyPrint() throws JsonProcessingException {
        String jsonString = pojoToPrettyString(getTestStaffMemberObject());

        DebugUtils.consoleHeader("Object mapper POJO -> String");
        System.out.println(jsonString);
        DebugUtils.newLine();

    }

    /**
     * Json string to map
     * @throws JsonProcessingException
     * @throws IOException
     */
    @Test
    public void stringToMap() throws JsonProcessingException, IOException{
        Map result = AerospikeDocumentClient.jsonStringToMap(pojoToString(getTestStaffMemberObject()));
        DebugUtils.consoleHeader("Read JSON string into map- show map.toString");
        System.out.println(result);
        DebugUtils.newLine();

    }

    /**
     * Convert POJO to map, save to Aerospike DB and read
     * @throws IOException
     */
    @Test
    public void writePOJOToDB() throws IOException{
        Map result = AerospikeDocumentClient.jsonStringToMap(pojoToString(getTestStaffMemberObject()));
        putMapToDB(result,TestConstants.TEST_AEROSPIKE_KEY);

        Map m = getMapFromDB(TestConstants.TEST_AEROSPIKE_KEY);
        deleteKey(TestConstants.TEST_AEROSPIKE_KEY);

        DebugUtils.consoleHeader("Putting and getting from the db");
        System.out.println(m);
        DebugUtils.newLine();
    }

    @Test
    public void demo() throws IOException, JsonPathParser.JsonParseException, AerospikeDocumentClient.AerospikeDocumentClientException {
        String jsonString = DebugUtils.readLineByLineJava("src/main/resources/tommy-lee-jones.json");
        final String AEROSPIKE_NAMESPACE = TestConstants.AEROSPIKE_NAMESPACE;
        final String AEROSPIKE_SET = TestConstants.AEROSPIKE_SET;

        // Put it in the DB
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(TestConstants.TEST_AEROSPIKE_CLIENT);

        Map jsonAsMap = AerospikeDocumentClient.jsonStringToMap(jsonString);
        Key tommyLeeJonesDBKey = new Key(AEROSPIKE_NAMESPACE,AEROSPIKE_SET,"tommy-lee-jones.json");
        documentClient.put(tommyLeeJonesDBKey, jsonAsMap);

        documentClient.put(tommyLeeJonesDBKey,"$.imdb_rank.rank",45);
        List<String> _2019Films = new Vector<String>();
        _2019Films.add("Ad Astra");
        documentClient.put(tommyLeeJonesDBKey,"$.selected_filmography.2019",_2019Films);

        documentClient.append(tommyLeeJonesDBKey,"$.best_films_ranked[0].films","Rolling Thunder");
        documentClient.append(tommyLeeJonesDBKey,"$.best_films_ranked[0].films","The Three Burials Of Melquiades Estrada");
        documentClient.delete(tommyLeeJonesDBKey,"$.best_films_ranked[1]");

        System.out.println(documentClient.get(tommyLeeJonesDBKey,"$.best_films_ranked[0].films[0]"));
        System.out.println(documentClient.get(tommyLeeJonesDBKey,"$.best_films_ranked[0].films[5]"));
        System.out.println(documentClient.get(tommyLeeJonesDBKey,"$.selected_filmography.2019"));

    }

    private static StaffMember getTestStaffMemberObject() {

        StaffMember staff = new StaffMember();

        staff.setName("mkyong");
        staff.setAge(38);
        staff.setPosition(new String[]{"Founder", "CTO", "Writer"});
        Map<String, BigDecimal> salary = new HashMap() {{
            put("2010", new BigDecimal(10000));
            put("2012", new BigDecimal(12000));
            put("2018", new BigDecimal(14000));
        }};
        staff.setSalary(salary);
        staff.setSkills(Arrays.asList("java", "python", "node", "kotlin"));

        return staff;

    }

    private static void putMapToDB(Map map,Key key){
        AerospikeClient client = TestConstants.TEST_AEROSPIKE_CLIENT;
        client.put(null,key,new Bin(TestConstants.JSON_EXAMPLE_BIN,map));

    }

    private static Map getMapFromDB(Key key) {
        Record r = TestConstants.TEST_AEROSPIKE_CLIENT.get(new Policy(), key);
        return r.getMap(TestConstants.JSON_EXAMPLE_BIN);
    }

    private static void deleteKey(Key key){
        TestConstants.TEST_AEROSPIKE_CLIENT.delete(getWritePolicy(),key);
    }

    private static WritePolicy getWritePolicy(){
        WritePolicy writePolicy = new WritePolicy();
        //writePolicy.durableDelete = true;
        return writePolicy;
    }

    private static String pojoToString(TestPOJO p) throws JsonProcessingException{
        ObjectMapper mapper = new ObjectMapper();
        // Java objects to JSON string - compact-print
        return mapper.writeValueAsString(p);
    }

    private static String pojoToPrettyString(TestPOJO p) throws JsonProcessingException{
        ObjectMapper mapper = new ObjectMapper();
        // Java objects to JSON string - compact-print
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(p);
    }

}
