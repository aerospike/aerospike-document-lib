package com.aerospike.documentAPI;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.*;

import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing JSON paths
 * Not useful outside of this package, hence package visibility
 */
public class JsonPathParser {
    static final String JSON_PATH_SEPARATOR = ".";
    static final String DOCUMENT_ROOT_TOKEN = "$";

    // Paths should match this pattern i.e. key[index1][index2]...
    static final Pattern PATH_PATTERN = Pattern.compile("^([^\\[^\\]]*)(\\[(\\d+)\\])*$");
    // This pattern to extract index1,index2 ...
    static final Pattern INDEX_PATTERN = Pattern.compile("(\\[(\\d+)\\])");
    
    /*
        Member variables
     */

    // Store our representation of the individual path parts
    List<PathPart> pathParts= new Vector<PathPart>();

    JsonPathParser(){}

    /**
     * Turn json path as string into PathPart recommendation
     * @param jsonString
     * @return List<PathPart></PathPart>
     * @throws JsonParseException
     */
    List<PathPart> parse (String jsonString) throws JsonParseException{
        if(jsonString.charAt(0) != '$') throw new JsonPrefixException(jsonString);
        StringTokenizer tokenizer = new StringTokenizer(jsonString,JSON_PATH_SEPARATOR);
        if(! tokenizer.nextToken().equals(DOCUMENT_ROOT_TOKEN)) throw new JsonPrefixException(jsonString);
        while(tokenizer.hasMoreTokens()){
            parsePathPart(tokenizer.nextToken());
        }
        return pathParts;
    }

    /**
     * Utility internal method to process the individual path parts
     * Appends the found path parts to the list of path parts already found
     * Expected form of pathPart is key[index1][index2]
     * @param pathPart
     * @throws JsonParseException
     */
    private void parsePathPart(String pathPart) throws JsonParseException{
        Matcher keyMatcher = PATH_PATTERN.matcher(pathPart);
        if((pathPart.indexOf("[") == -1) & (pathPart.indexOf("]") == -1)){
            pathParts.add(new MapPart(pathPart));
        }
        else if(keyMatcher.find()) {
            String key = keyMatcher.group(1);
            pathParts.add(new MapPart(key));
            Matcher indexMatcher = INDEX_PATTERN.matcher(pathPart);
            while (indexMatcher.find()) {
                pathParts.add(new ListPart(Integer.parseInt(indexMatcher.group(2))));
            }
        }
        else {
            throw new JsonPathException(pathPart);
        }
    }

    /**
     * PathPart analysis is ultimately used to create CTX (context) objects and operations
     */
    abstract class PathPart {
        abstract CTX toAerospikeContext();
        abstract Operation toAerospikeGetOperation(String binName, CTX[] contexts);
        abstract Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts);
        public Operation toAerospikeAppendOperation(String binName,Object object,CTX[] contexts){
            return ListOperation.append(binName,Value.get(object),contexts);
        }
        abstract Operation toAerospikeDeleteOperation(String binName,CTX[] contexts);
    }

    /**
     * MapPart is a representation of key access
     */
    class MapPart extends PathPart {
        String key;

        MapPart(String key){
            this.key = key;
        }

        String getKey(){
            return key;
        }

        boolean equals(MapPart m){
            return m.key.equals(key);
        }

        public CTX toAerospikeContext(){
            return CTX.mapKey(Value.get(key));
        }

        public Operation toAerospikeGetOperation(String binName, CTX[] contexts){
            return MapOperation.getByKey(binName,Value.get(key),MapReturnType.VALUE,contexts);
        }

        public Operation toAerospikePutOperation(String binName,Object object,CTX[] contexts){
            return MapOperation.put(new MapPolicy(),binName,Value.get(key),Value.get(object),contexts);
        }

        public Operation toAerospikeDeleteOperation(String binName,CTX[] contexts){
            return MapOperation.removeByKey(binName,Value.get(key),MapReturnType.NONE,contexts);
        }
    }

    /**
     * A ListPart is a representation of a list access
     */
    class ListPart extends PathPart {
        int listPosition;

        ListPart(int listPosition){
            this.listPosition = listPosition;
        }

        int getListPosition(){
            return listPosition;
        }

        boolean equals(ListPart l){
            return l.listPosition == listPosition;
        }

        public CTX toAerospikeContext(){
            return CTX.listIndex(listPosition);
        }

        public Operation toAerospikeGetOperation(String binName, CTX[] contexts){
            return ListOperation.getByIndex(binName,listPosition,ListReturnType.VALUE,contexts);
        }

        public Operation toAerospikePutOperation(String binName,Object object,CTX[] contexts){
            return ListOperation.insert(binName,listPosition,Value.get(object),contexts);
        }

        public Operation toAerospikeDeleteOperation(String binName,CTX[] contexts){
            return ListOperation.removeByIndex(binName,listPosition,ListReturnType.NONE,contexts);
        }
    }

    /**
     * Given a list of path parts, convert this to the list of contexts you would need
     * to retrieve the JSON path represented by the list of path parts
     * @param pathParts
     * @return
     */
    public static List<CTX> pathPartsToContexts(List<PathPart> pathParts){
        List<CTX> contextList = new Vector<CTX>();
        Iterator<PathPart> pathPartIterator = pathParts.iterator();
        while(pathPartIterator.hasNext()) contextList.add(pathPartIterator.next().toAerospikeContext());
        return contextList;
    }

    /**
     * Different types of json path exception
     */
    public static abstract class JsonParseException extends Exception{
        String jsonString;
        JsonParseException(String s){
            jsonString = s;
        }
    }

    public static class JsonPrefixException extends JsonParseException{
        JsonPrefixException(String s){
            super(s);
        }

        public String toString(){
            return jsonString + " should start with a $";
        }
    }

    public static class JsonPathException extends JsonParseException{
        JsonPathException(String s){
            super(s);
        }

        public String toString(){
            return jsonString + " does not match key[number] format";
        }
    }

    public static class ListException extends JsonParseException{
        ListException(String s){super(s);}

        public String toString(){return "You can't append to a document root";}

    }
}
