package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.ResultCode;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.documentapi.data.DocumentFilter;
import com.aerospike.documentapi.data.DocumentQueryStatement;
import com.aerospike.documentapi.data.KeyResult;
import com.aerospike.documentapi.util.ExpConverter;
import net.minidev.json.JSONArray;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.aerospike.documentapi.data.Operator.EQ;
import static com.aerospike.documentapi.data.Operator.GT;
import static com.aerospike.documentapi.data.Operator.GTE;
import static com.aerospike.documentapi.data.Operator.REGEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentExpTests extends BaseTestConfig {

    private static final Key QUERY_KEY_1 = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "key1");
    private static final Key QUERY_KEY_2 = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "key2");
    private static final String MAP_BIN_NAME = "mapBin";
    private static final String LIST_BIN_NAME = "listBin";

    private final Bin mapBin1 = new Bin(MAP_BIN_NAME, mapBin(0));
    private final Bin listBin1 = new Bin(LIST_BIN_NAME, listBin(0));
    private final Bin mapBin2 = new Bin(MAP_BIN_NAME, mapBin(100));
    private final Bin listBin2 = new Bin(LIST_BIN_NAME, listBin(100));

    @BeforeAll
    void setUp() {
        client.put(writePolicy(), QUERY_KEY_1, mapBin1, listBin1);
        client.put(writePolicy(), QUERY_KEY_2, mapBin2, listBin2);
        // create collection index on mapBin
        createIndex(client, AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "mapkey_index", MAP_BIN_NAME);
    }

    @AfterAll
    void tearDown() {
        client.delete(null, QUERY_KEY_1);
        client.delete(null, QUERY_KEY_2);
        client.dropIndex(null, AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "mapkey_index");
    }

    @Test
    void queryList() throws DocumentApiException {
        String jsonPath = "$.listKey[0].k11";
        Exp exp = ExpConverter.lt(MAP_BIN_NAME, jsonPath, 100);
        QueryPolicy queryPolicy = new QueryPolicy(writePolicy());
        queryPolicy.filterExp = Exp.build(exp);

        List<KeyRecord> keyRecords = recordSetToList(client.query(queryPolicy, statement()));
        assertEquals(1, keyRecords.size());
        assertEquals("key1", keyRecords.get(0).key.userKey.getObject());
    }

    @Test
    void queryMap() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11";
        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .jsonPaths(new String[]{jsonPath})
                .build();

        DocumentFilter filterExp = new DocumentFilter(MAP_BIN_NAME, jsonPath, GTE, 100);
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(1, keyResults.size());
        assertEquals("key2", keyResults.get(0).getKey().userKey.toString());
    }

    @Test
    void queryMapSecondaryIndex() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11";
        DocumentFilter secIndexFilter = new DocumentFilter(MAP_BIN_NAME, jsonPath, GTE, 100);
        secIndexFilter.setIndexCollectionType(IndexCollectionType.MAPKEYS);

        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .documentFilter(secIndexFilter)
                .build();

        DocumentFilter filterExp = null; // TODO: ability to apply secIndexFilter and FilterExp separately?
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(2, keyResults.size());
    }

    @Test
    void queryMapSecondaryIndexNoMatch() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11";
        // secIndexFilter should find nothing as there are only 11 and 111 values of k11
        DocumentFilter secIndexFilter = new DocumentFilter(MAP_BIN_NAME, jsonPath, EQ, 100);
//        DocumentFilter secIndexFilter = new DocumentFilter(MAP_BIN_NAME, jsonPath, EQ, "100");
        secIndexFilter.setIndexCollectionType(IndexCollectionType.MAPKEYS);

        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .documentFilter(secIndexFilter)
                .build();

        DocumentFilter filterExp = null;
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(0, keyResults.size());
    }

    @Test
    void queryMapSecondaryIndexJsonPathFilterExp() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11";
        DocumentFilter filter = new DocumentFilter(MAP_BIN_NAME, jsonPath, GTE, 100);
        filter.setIndexCollectionType(IndexCollectionType.MAPKEYS);

        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .jsonPaths(new String[]{jsonPath})
                .documentFilter(filter)
                .build();

        Stream<KeyResult> test = documentClient.query(queryStatement, filter);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(1, keyResults.size());
        assertEquals("key2", keyResults.get(0).getKey().userKey.toString());
    }

    @Test
    void queryMapMultipleBins() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11";
        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .binNames(new String[]{LIST_BIN_NAME, MAP_BIN_NAME})
                .jsonPaths(new String[]{jsonPath})
                .build();

        DocumentFilter filterExp = new DocumentFilter(MAP_BIN_NAME, jsonPath, GTE, 100);
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(1, keyResults.size());
    }

    @Test
    void queryMapEmptyResult() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11"; // value exists in MapBin
        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .binNames(new String[]{LIST_BIN_NAME}) // requiring only ListBin
                .jsonPaths(new String[]{jsonPath})
                .build();

        DocumentFilter filterExp = new DocumentFilter(MAP_BIN_NAME, jsonPath, GTE, 100);
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(0, keyResults.size());
    }

    @Test
    void queryMapMultiplePathsNoFilterExp() throws DocumentApiException {
        String jsonPathMapKey = "$.mapKey.k1.k11";
        String jsonPathListKey = "$.listKey[0].k11";
        String jsonPathListBin = "$[0]]";

        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .jsonPaths(new String[]{jsonPathMapKey, jsonPathListKey, jsonPathListBin})
                .build();

        DocumentFilter filterExp = null;
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(2, keyResults.size());
        //noinspection unchecked
        assertEquals(3, ((Map<String, Object>) keyResults.get(0).getResult()).keySet().size());
        assertEquals(keyResults.get(0).getResult(), keyResults.get(0).getResult());
    }

    @Test
    void queryMapMultiplePathsAndFilterExps() throws DocumentApiException {
        String jsonPathMapKey = "$.mapKey.k1.k11";
        String jsonPathListKey = "$.listKey[0].k11";
        String jsonPathListBin = "$[0]]";

        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .jsonPaths(new String[]{jsonPathMapKey, jsonPathListKey, jsonPathListBin})
                .build();

        DocumentFilter filterExpMapKeyGte100 = new DocumentFilter(MAP_BIN_NAME, jsonPathMapKey, GTE, 100);
        DocumentFilter filterExpListKeyGte100 = new DocumentFilter(MAP_BIN_NAME, jsonPathListKey, GT, 100);
        DocumentFilter filterExpListBin = new DocumentFilter(LIST_BIN_NAME, jsonPathListBin, GTE, 100);
        Stream<KeyResult> test = documentClient.query(queryStatement,
                filterExpMapKeyGte100, filterExpListKeyGte100, filterExpListBin);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(1, keyResults.size());
        assertEquals("key2", keyResults.get(0).getKey().userKey.toString());
    }

    @Test
    void queryFilter() throws DocumentApiException {
        String jsonPath = "$.listKey[?(@.k11 < 20)]";

        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .jsonPaths(new String[]{jsonPath})
                .build();

        DocumentFilter filterExp = null;
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(1, keyResults.size());

        @SuppressWarnings("unchecked")
        List<KeyResult> nonEmptyKeyResults = keyResults.stream()
                .filter(keyRes -> ((Map<String, JSONArray>) keyRes.getResult()).values()
                        .stream()
                        .anyMatch(arr -> !arr.isEmpty()))
                .collect(Collectors.toList());
        assertEquals(1, nonEmptyKeyResults.size());

        @SuppressWarnings("unchecked")
        List<JSONArray> jsonArrays = new ArrayList<>(((Map<String, JSONArray>)
                nonEmptyKeyResults.get(0).getResult()).values());
        assertEquals(1, jsonArrays.size());
        Map<?, ?> result = ((Map<?, ?>) jsonArrays.get(0).get(0));
        assertEquals(2, result.keySet().size());
        assertTrue(result.containsKey("k12"));
        assertTrue(result.containsKey("k11"));
        assertEquals("key1", nonEmptyKeyResults.get(0).getKey().userKey.toString());
    }

    @Test
    void queryListRegex() throws DocumentApiException {
        String jsonPath = "$.listKey[2]";
        DocumentQueryStatement queryStatement = DocumentQueryStatement.builder()
                .namespace(AEROSPIKE_NAMESPACE)
                .setName(AEROSPIKE_SET)
                .jsonPaths(new String[]{jsonPath})
                .build();

        DocumentFilter filterExp = new DocumentFilter(MAP_BIN_NAME, jsonPath, REGEX, "10.*");
        filterExp.setRegexFlags(RegexFlag.ICASE);
        Stream<KeyResult> test = documentClient.query(queryStatement, filterExp);
        List<KeyResult> keyResults = test.collect(Collectors.toList());
        assertEquals(1, keyResults.size());
        assertEquals("key2", keyResults.get(0).getKey().userKey.getObject());
    }

    @Test
    void queryRootList() throws DocumentApiException {
        String jsonPath = "$[1]";
        Exp exp = ExpConverter.ne(LIST_BIN_NAME, jsonPath, 102);
        QueryPolicy queryPolicy = new QueryPolicy(writePolicy());
        queryPolicy.filterExp = Exp.build(exp);

        List<KeyRecord> keyRecords = recordSetToList(client.query(queryPolicy, statement()));
        assertEquals(1, keyRecords.size());
        assertEquals("key1", keyRecords.get(0).key.userKey.getObject());
    }

    @Test
    void testInvalidJsonPath() {
        String jsonPath = "abc";
        assertThrows(
                DocumentApiException.class,
                () -> ExpConverter.eq(MAP_BIN_NAME, jsonPath, 100)
        );
    }

    @Test
    void testTwoStepJsonPath() {
        String jsonPath = "$.listKey[*]";
        assertThrows(
                IllegalArgumentException.class,
                () -> ExpConverter.gt(MAP_BIN_NAME, jsonPath, 100)
        );
    }

    private Statement statement() {
        Statement statement = new Statement();
        statement.setNamespace(AEROSPIKE_NAMESPACE);
        statement.setSetName(AEROSPIKE_SET);
        return statement;
    }

    private WritePolicy writePolicy() {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;
        return writePolicy;
    }

    private List<KeyRecord> recordSetToList(RecordSet recordSet) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        recordSet.iterator(),
                        Spliterator.ORDERED
                ), false)
                .collect(Collectors.toList());
    }

    private Map<String, Object> mapBin(int offset) {
        Map<String, Object> mapBin = new HashMap<>();
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("k1", Stream.of(new Object[][]{
                {"k11", offset + 11},
                {"k12", offset + 12},
        }).collect(Collectors.toMap(data -> data[0], data -> data[1])));
        mapBin.put("mapKey", innerMap);
        List<Object> innerList = new ArrayList<>();
        innerList.add(Stream.of(new Object[][]{
                {"k11", offset + 11},
                {"k12", offset + 12},
        }).collect(Collectors.toMap(data -> data[0], data -> data[1])));
        innerList.add(Stream.of(new Object[][]{
                {"k13", offset + 13},
                {"k14", offset + 14},
        }).collect(Collectors.toMap(data -> data[0], data -> data[1])));
        innerList.add(String.format("%d", offset));
        mapBin.put("listKey", innerList);
        return mapBin;
    }

    private List<Integer> listBin(int offset) {
        return Arrays.asList(
                1 + offset,
                2 + offset,
                3 + offset
        );
    }

    @SuppressWarnings("SameParameterValue")
    private void createIndex(
            IAerospikeClient client,
            String namespace,
            String set,
            String indexName,
            String binName
    ) throws RuntimeException {
        Policy policy = new Policy();
        policy.socketTimeout = 0; // Do not time out on index create.

        try {
            IndexTask task = client.createIndex(policy, namespace, set, indexName, binName,
                    IndexType.STRING, IndexCollectionType.MAPKEYS);
            task.waitTillComplete();
        } catch (AerospikeException ae) {
            if (ae.getResultCode() != ResultCode.INDEX_ALREADY_EXISTS) {
                throw ae;
            }
        }
    }
}
