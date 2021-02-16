package com.aerospike.documentAPI;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.fail;

public class JsonPathParserTest {
    /**
     * Check that a parse error is thrown if json path does not start with a $
     */
    @Test
    public void throwErrorIfNotDollarStart(){
        String testPath = ".this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        }
        catch(JsonPathParser.JsonParseException e){

        }
    }

    /**
     * Check that a parse error is thrown if json path does not start with a $.
     */
    @Test
    public void throwErrorIfNotDollarDotStart(){
        String testPath = "$x.this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        }
        catch(JsonPathParser.JsonParseException e){
        }
    }

    /**
     * Check that an error is not thrown if json path starts with a $
     *
     */
    @Test
    public void noThrowErrorIfNotDollarStart() throws JsonPathParser.JsonParseException {
        String testPath = "$.this.that";
        JsonPathParser parser = new JsonPathParser();
        try {
            parser.parse(testPath);
        }
        catch(JsonPathParser.JsonParseException e){
            fail("Should not be here, parser should not have thrown error");
        }
    }
    @Test
    /**
     * Is $.key parsed correctly
     */
    public void parsesGoodPath1(){
        String testPath = "$.key";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
        }
        catch(JsonPathParser.JsonParseException e){
            fail("Should not be here, parser should not have thrown error");
        }
        Assert.assertTrue(pathParts.size() == 1);
        Assert.assertTrue(((JsonPathParser.MapPart) pathParts.get(0)).equals(parser.new MapPart("key")));
    }

    @Test
    /**
     * Is $.key[2] parsed correctly
     */
    public void parsesGoodPath2() {
        String testPath = "$.key[2]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
        }
        catch (JsonPathParser.JsonParseException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        Assert.assertTrue(pathParts.size() == 2);
        Assert.assertTrue(((JsonPathParser.MapPart) pathParts.get(0)).equals(parser.new MapPart("key")));
        Assert.assertTrue(((JsonPathParser.ListPart) pathParts.get(1)).equals(parser.new ListPart(2)));
    }

    @Test
    /**
     * Is $.key[1][2] parsed correctly
     */
    public void parsesGoodPath3() {
        String testPath = "$.key[1][2]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
        }
        catch (JsonPathParser.JsonParseException e) {
            fail("Should not be here, parser should not have thrown error");
        }
        Assert.assertTrue(pathParts.size() == 3);
        Assert.assertTrue(((JsonPathParser.MapPart) pathParts.get(0)).equals(parser.new MapPart("key")));
        Assert.assertTrue(((JsonPathParser.ListPart) pathParts.get(1)).equals(parser.new ListPart(1)));
        Assert.assertTrue(((JsonPathParser.ListPart) pathParts.get(2)).equals(parser.new ListPart(2)));

    }

    @Test
    /**
     * Check that we fail parsing $.key[
     */
    public void parseFailsBadPath1(){
        String testPath = "$.key[";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        }
        catch(JsonPathParser.JsonParseException e){
        }
    }

    @Test
    /**
     * Check that we fail parsing $.key[]
     */
    public void parseFailsBadPath2(){
        String testPath = "$.key[]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        }
        catch(JsonPathParser.JsonParseException e){
        }
    }

    @Test
    /**
     * Check that we fail parsing $.key[a]
     */
    public void parseFailsBadPath3(){
        String testPath = "$.key[a]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        }
        catch(JsonPathParser.JsonParseException e){
        }
    }

    @Test
    /**
     * Check we fail if parsing $.key]
     */
    public void parseFailsBadPath4(){
        String testPath = "$.key]";
        JsonPathParser parser = new JsonPathParser();
        List<JsonPathParser.PathPart> pathParts = null;
        try {
            pathParts = parser.parse(testPath);
            fail("Should not be here, parser should have thrown error");
        }
        catch(JsonPathParser.JsonParseException e){
        }
    }
}
