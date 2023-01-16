package com.aerospike.documentapi;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.RegexFlag;
import com.aerospike.client.query.Statement;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.token.ContextAwareToken;
import com.aerospike.documentapi.token.Token;
import com.aerospike.documentapi.token.filterExpr.FilterExp;
import com.aerospike.documentapi.util.DocumentExp;
import com.aerospike.documentapi.util.JsonConverters;
import com.fasterxml.jackson.databind.JsonNode;
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

import static com.aerospike.documentapi.util.DocumentExp.validateJsonPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DocumentExpTests extends BaseTestConfig {

    private static final Key QUERY_KEY_1 = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "key1");
    private static final Key QUERY_KEY_2 = new Key(AEROSPIKE_NAMESPACE, AEROSPIKE_SET, "key2");
    private static final String MAP_BIN_NAME = "mapBin";
    private static final String LIST_BIN_NAME = "listBin";

    @BeforeAll
    void setUp() {
        client.put(writePolicy(), QUERY_KEY_1, new Bin(MAP_BIN_NAME, mapBin(0)),
                new Bin(LIST_BIN_NAME, listBin(0)));
        client.put(writePolicy(), QUERY_KEY_2, new Bin(MAP_BIN_NAME, mapBin(100)),
                new Bin(LIST_BIN_NAME, listBin(100)));
        AerospikeDocumentClient documentClient = new AerospikeDocumentClient(client);
        JsonNode jsonNode = JsonConverters.convertStringToJsonNode(storeJson);
        documentClient.put(TEST_AEROSPIKE_KEY, DOCUMENT_BIN_NAME, jsonNode);
    }

    @AfterAll
    void tearDown() {
        client.delete(null, QUERY_KEY_1);
        client.delete(null, QUERY_KEY_2);
    }

    @Test
    void testDocumentExpMap() throws DocumentApiException {
        String jsonPath = "$.mapKey.k1.k11";
        Exp exp = DocumentExp.ge(MAP_BIN_NAME, jsonPath, 100);
        QueryPolicy queryPolicy = new QueryPolicy(writePolicy());
        queryPolicy.filterExp = Exp.build(exp);

        List<KeyRecord> keyRecords = recordSetToList(client.query(queryPolicy, statement()));
        assertEquals(1, keyRecords.size());
        assertEquals("key2", keyRecords.get(0).key.userKey.getObject());
    }

    @Test
    void testDocumentExpList() throws DocumentApiException {
        String jsonPath = "$.listKey[0].k11";
        Exp exp = DocumentExp.lt(MAP_BIN_NAME, jsonPath, 100);
        QueryPolicy queryPolicy = new QueryPolicy(writePolicy());
        queryPolicy.filterExp = Exp.build(exp);

        List<KeyRecord> keyRecords = recordSetToList(client.query(queryPolicy, statement()));
        assertEquals(1, keyRecords.size());
        assertEquals("key1", keyRecords.get(0).key.userKey.getObject());
    }

    @Test
    void testDocumentExpFilter() throws DocumentApiException {
        String jsonPath = "$.store.book[?(@.price > 10)]";
//        JsonPathObject jsonPathObject = validateJsonPath(new JsonPathParser().parse(jsonPath));
        JsonPathObject jsonPathObject = new JsonPathParser().parse(jsonPath);
        Exp exp = null;
        int tokensSize = jsonPathObject.getTokensRequiringSecondStepQuery().size();
        FilterExp filterCriteria = tokensSize > 0 ?
                jsonPathObject.getTokensRequiringSecondStepQuery().get(tokensSize - 1).getFilterCriteria()
                : null;
        if (filterCriteria != null && filterCriteria.getValues().length >= 1) {
            if (filterCriteria.getValues().length == 2) {
                FilterExp rightOperand = (FilterExp) filterCriteria.getValues()[1];
                List<Token> leftOperandTokens = ((FilterExp) filterCriteria.getValues()[0]).getTokens();
                exp = DocumentExp.ge(DOCUMENT_BIN_NAME, jsonPathObject, leftOperandTokens, rightOperand.getValues()[0]);
            }
        }
        QueryPolicy queryPolicy = new QueryPolicy(writePolicy());
        queryPolicy.filterExp = Exp.build(exp);

        List<KeyRecord> keyRecords = recordSetToList(client.query(queryPolicy, statement()));
        assertEquals(1, keyRecords.size());
        assertEquals("key2", keyRecords.get(0).key.userKey.getObject());
    }

    @Test
    void testDocumentExpListRegex() throws DocumentApiException {
        String jsonPath = "$.listKey[2]";
        Exp exp = DocumentExp.regex(MAP_BIN_NAME, jsonPath, "10.*", RegexFlag.ICASE);
        QueryPolicy queryPolicy = new QueryPolicy(writePolicy());
        queryPolicy.filterExp = Exp.build(exp);

        List<KeyRecord> keyRecords = recordSetToList(client.query(queryPolicy, statement()));
        assertEquals(1, keyRecords.size());
        assertEquals("key2", keyRecords.get(0).key.userKey.getObject());
    }

    @Test
    void testDocumentExpRootList() throws DocumentApiException {
        String jsonPath = "$[1]";
        Exp exp = DocumentExp.ne(LIST_BIN_NAME, jsonPath, 102);
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
                () -> DocumentExp.eq(MAP_BIN_NAME, jsonPath, 100)
        );
    }

    @Test
    void testTwoStepJsonPath() {
        String jsonPath = "$.listKey[*]";
        assertThrows(
                IllegalArgumentException.class,
                () -> DocumentExp.gt(MAP_BIN_NAME, jsonPath, 100)
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
}
