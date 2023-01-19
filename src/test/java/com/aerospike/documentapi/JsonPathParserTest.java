package com.aerospike.documentapi;

import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.token.ListToken;
import com.aerospike.documentapi.token.MapToken;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class JsonPathParserTest {

    /**
     * Check that a parse error is thrown if json path does not start with a $.
     */
    @Test
    void throwErrorIfNotDollarStart() {
        String testPath = ".this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Check that a parse error is thrown if json path does not start with a $.
     */
    @Test
    void throwErrorIfNotDollarDotStart() {
        String testPath = "$x.this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Check that an error is not thrown if json path starts with a $.
     */
    @Test
    void noThrowErrorIfNotDollarStart() {
        String testPath = "$.this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
        } catch (DocumentApiException e) {
            fail("Should not be here, parser should not have thrown error");
        }
    }

    /**
     * Verify that $[1] is parsed correctly.
     */
    @Test
    void parsesRootListPath() {
        String testPath = "$[1]";
        JsonPathParser parser = new JsonPathParser();
        JsonPathObject jsonPathObject = new JsonPathObject();
        try {
            jsonPathObject = parser.parse(testPath);
        } catch (DocumentApiException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        assertEquals(1, jsonPathObject.getTokensNotRequiringSecondStepQuery().size());
        assertEquals(new ListToken(1), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(0));
    }

    /**
     * Verify that $.key is parsed correctly.
     */
    @Test
    void parsesGoodPath1() {
        String testPath = "$.key";
        JsonPathParser parser = new JsonPathParser();
        JsonPathObject jsonPathObject = new JsonPathObject();
        try {
            jsonPathObject = parser.parse(testPath);
        } catch (DocumentApiException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        assertEquals(1, jsonPathObject.getTokensNotRequiringSecondStepQuery().size());
        assertEquals(new MapToken("key"), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(0));
    }

    /**
     * Verify that $.key[2] is parsed correctly.
     */
    @Test
    void parsesGoodPath2() {
        String testPath = "$.key[2]";
        JsonPathParser parser = new JsonPathParser();
        JsonPathObject jsonPathObject = new JsonPathObject();
        try {
            jsonPathObject = parser.parse(testPath);
        } catch (DocumentApiException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        assertEquals(2, jsonPathObject.getTokensNotRequiringSecondStepQuery().size());
        assertEquals(new MapToken("key"), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(0));
        assertEquals(new ListToken(2), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(1));
    }

    /**
     * Verify that $.key[1][2] is parsed correctly.
     */
    @Test
    void parsesGoodPath3() {
        String testPath = "$.key[1][2]";
        JsonPathParser parser = new JsonPathParser();
        JsonPathObject jsonPathObject = new JsonPathObject();
        try {
            jsonPathObject = parser.parse(testPath);
        } catch (DocumentApiException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        assertEquals(3, jsonPathObject.getTokensNotRequiringSecondStepQuery().size());
        assertEquals(new MapToken("key"), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(0));
        assertEquals(new ListToken(1), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(1));
        assertEquals(new ListToken(2), jsonPathObject.getTokensNotRequiringSecondStepQuery().get(2));
    }

    /**
     * Check that we fail parsing $.key[.
     */
    @Test
    void parseFailsBadPath1() {
        String testPath = "$.key[";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Check that we fail parsing $.key[].
     */
    @Test
    void parseFailsBadPath2() {
        String testPath = "$.key[]";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Check that we fail parsing $.key[a].
     */
    @Test
    void parseFailsBadPath3() {
        String testPath = "$.key[a]";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (DocumentApiException ignored) {
        }
    }

    /**
     * Check we fail if parsing $.key].
     */
    @Test
    void parseFailsBadPath4() {
        String testPath = "$.key]";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        } catch (DocumentApiException ignored) {
        }
    }
}
