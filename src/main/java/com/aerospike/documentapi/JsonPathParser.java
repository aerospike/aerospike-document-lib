package com.aerospike.documentapi;

import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.pathparts.ListPathPart;
import com.aerospike.documentapi.pathparts.MapPathPart;
import com.aerospike.documentapi.pathparts.PathPart;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
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

    private final List<String> jsonPathQueryIndications = Arrays.asList(
            "[*]",
            "..",
            "[?",
            ".min()",
            ".max()",
            ".avg()",
            ".stddev()",
            ".length()"
    );

    // Store our representation of the individual path parts
    JsonPathObject jsonPathObject = new JsonPathObject();

    JsonPathParser() {
    }

    /**
     * Turn json path as string into PathPart recommendation
     *
     * @param jsonString a given JSON as String
     * @return List<PathPart></PathPart>
     * @throws JsonParseException a JsonParseException will be thrown in case of an error.
     */
    JsonPathObject parse(String jsonString) throws JsonParseException {
        if (jsonString.charAt(0) != '$') {
            throw new JsonPrefixException(jsonString);
        }
        StringTokenizer tokenizer = new StringTokenizer(jsonString, JSON_PATH_SEPARATOR);
        if (!tokenizer.nextToken().equals(DOCUMENT_ROOT_TOKEN)) {
            throw new JsonPrefixException(jsonString);
        }

        Integer index = getFirstIndexOfAQueryIndication(jsonString);
        // Query is required
        if (index != null) {
            /*
                Split the jsonString into 2 parts:
                    1. A string of path parts before a query operator to fetch the smallest Json possible from Aerospike.
                    2. A string of the remaining JsonPath to later use for executing a JsonPath query on the fetched Json from Aerospike.

                For example:
                $.store.book[*].author
                store.book will be fetched from Aerospike and a JsonPath book[*].author query will be executed on the fetched results
                from Aerospike (key = book, value = nested Json).
            */
            jsonPathObject.setRequiresJsonPathQuery(true);
            String aerospikePathPartsString = jsonString.substring(0, index);
            String jsonPathPathPartsString = jsonString.substring(index);
            jsonPathObject.setJsonPathSecondStepQuery(jsonPathPathPartsString);
            tokenizer = new StringTokenizer(aerospikePathPartsString, JSON_PATH_SEPARATOR);
            if (!tokenizer.nextToken().equals(DOCUMENT_ROOT_TOKEN)) {
                throw new JsonPrefixException(jsonString);
            }
        }

        while (tokenizer.hasMoreTokens()) {
            parsePathPart(tokenizer.nextToken());
        }

        return jsonPathObject;
    }

    /**
     * Utility internal method to process the individual path parts
     * Appends the found path parts to the list of path parts already found
     * Expected form of pathPart is key[index1][index2]
     *
     * @param pathPart pathPart to Parse.
     * @throws JsonParseException a JsonParseException will be thrown in case of an error.
     */
    private void parsePathPart(String pathPart) throws JsonParseException {
        Matcher keyMatcher = PATH_PATTERN.matcher(pathPart);
        if ((!pathPart.contains("[")) && (!pathPart.contains("]"))) {
            // ignore * wildcard after a dot, its the same as ending with a .path
            if (!pathPart.equals("*")) {
                jsonPathObject.addPathPart(new MapPathPart(pathPart));
            }
        } else if (keyMatcher.find()) {
            String key = keyMatcher.group(1);
            jsonPathObject.addPathPart(new MapPathPart(key));
            Matcher indexMatcher = INDEX_PATTERN.matcher(pathPart);

            while (indexMatcher.find()) {
                jsonPathObject.addPathPart(new ListPathPart(Integer.parseInt(indexMatcher.group(2))));
            }
        } else {
            throw new JsonPathException(pathPart);
        }
    }

    public static PathPart extractLastPathPart(List<PathPart> pathParts) {
        return pathParts.get(pathParts.size() - 1);
    }

    public static PathPart extractLastPathPartAndModifyList(List<PathPart> pathParts) {
        return pathParts.remove(pathParts.size() - 1);
    }

    /**
     * Given a list of path parts, convert this to the list of contexts you would need
     * to retrieve the JSON path represented by the list of path parts
     *
     * @param pathParts pathParts list to convert.
     * @return An array of contexts (CTXs).
     */
    public static CTX[] pathPartsToContextsArray(List<PathPart> pathParts) {
        List<CTX> contextList = new ArrayList<>();
        for (PathPart pathPart : pathParts) {
            contextList.add(pathPart.toAerospikeContext());
        }
        return contextList.toArray(new CTX[contextList.size()]);
    }

    private Integer getFirstIndexOfAQueryIndication(String jsonPath) {
        return jsonPathQueryIndications.stream()
                .map(jsonPath::indexOf)
                .filter(index -> index > 0)
                .min(Integer::compare)
                .orElse(null); // in case there no match for a query indication
    }

    /**
     * Different types of json path exception
     */
    public abstract static class JsonParseException extends Exception {
        final String jsonString;

        JsonParseException(String s) {
            jsonString = s;
        }
    }

    public static class JsonPrefixException extends JsonParseException {
        JsonPrefixException(String s) {
            super(s);
        }

        @Override
        public String toString() {
            return jsonString + " should start with a $";
        }
    }

    public static class JsonPathException extends JsonParseException {
        JsonPathException(String s) {
            super(s);
        }

        @Override
        public String toString() {
            return jsonString + " does not match key[number] format";
        }
    }

    public static class ListException extends JsonParseException {
        ListException(String s) {
            super(s);
        }

        @Override
        public String toString() {
            return "You can't append to a document root";
        }
    }
}
