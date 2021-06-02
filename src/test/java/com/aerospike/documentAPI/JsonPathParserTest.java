package com.aerospike.documentAPI;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.fail;

public class JsonPathParserTest {

    /**
     * Check that a parse error is thrown if json path does not start with a $.
     */
    @Test
    public void throwErrorIfNotDollarStart() {
        String testPath = ".this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (JsonPathParser.JsonParseException ignored) {
        }
    }

    /**
     * Check that a parse error is thrown if json path does not start with a $.
     */
    @Test
    public void throwErrorIfNotDollarDotStart() {
        String testPath = "$x.this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (JsonPathParser.JsonParseException ignored) {
        }
    }

    /**
     * Check that an error is not thrown if json path starts with a $.
     */
    @Test
    public void noThrowErrorIfNotDollarStart() {
        String testPath = "$.this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
        } catch (JsonPathParser.JsonParseException e) {
            fail("Should not be here, parser should not have thrown error");
        }
    }

    /**
     * Verify that $.key is parsed correctly.
     */
    @Test
    public void parsesGoodPath1() {
        String testPath = "$.key";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
        } catch (JsonPathParser.JsonParseException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        Assert.assertEquals(1, pathParts.size());
        Assert.assertTrue(((JsonPathParser.MapPart) pathParts.get(0)).equals(parser.new MapPart("key")));
    }

    /**
     * Verify that $.key[2] is parsed correctly.
     */
    @Test
    public void parsesGoodPath2() {
        String testPath = "$.key[2]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
        } catch (JsonPathParser.JsonParseException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        Assert.assertEquals(2, pathParts.size());
        Assert.assertTrue(((JsonPathParser.MapPart) pathParts.get(0)).equals(parser.new MapPart("key")));
        Assert.assertTrue(((JsonPathParser.ListPart) pathParts.get(1)).equals(parser.new ListPart(2)));
    }

    /**
     * Verify that $.key[1][2] is parsed correctly.
     */
    @Test
    public void parsesGoodPath3() {
        String testPath = "$.key[1][2]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
        } catch (JsonPathParser.JsonParseException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        Assert.assertEquals(3, pathParts.size());
        Assert.assertTrue(((JsonPathParser.MapPart) pathParts.get(0)).equals(parser.new MapPart("key")));
        Assert.assertTrue(((JsonPathParser.ListPart) pathParts.get(1)).equals(parser.new ListPart(1)));
        Assert.assertTrue(((JsonPathParser.ListPart) pathParts.get(2)).equals(parser.new ListPart(2)));

    }

    /**
     * Check that we fail parsing $.key[.
     */
    @Test
    public void parseFailsBadPath1() {
        String testPath = "$.key[";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (JsonPathParser.JsonParseException ignored) {
        }
    }

    /**
     * Check that we fail parsing $.key[].
     */
    @Test
    public void parseFailsBadPath2() {
        String testPath = "$.key[]";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (JsonPathParser.JsonParseException ignored) {
        }
    }

    /**
     * Check that we fail parsing $.key[a].
     */
    @Test
    public void parseFailsBadPath3() {
        String testPath = "$.key[a]";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (JsonPathParser.JsonParseException ignored) {
        }
    }

    /**
     * Check we fail if parsing $.key].
     */
    @Test
    public void parseFailsBadPath4() {
        String testPath = "$.key]";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (JsonPathParser.JsonParseException ignored) {
        }
    }
}
