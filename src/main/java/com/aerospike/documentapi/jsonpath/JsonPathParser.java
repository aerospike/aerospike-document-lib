package com.aerospike.documentapi.jsonpath;

import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.DocumentApiException;
import com.aerospike.documentapi.jsonpath.pathpart.ListPathPart;
import com.aerospike.documentapi.jsonpath.pathpart.MapPathPart;
import com.aerospike.documentapi.jsonpath.pathpart.PathPart;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing JSONPath strings.
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
    private final JsonPathObject jsonPathObject;

    public JsonPathParser() {
        this.jsonPathObject = new JsonPathObject();
    }

    public static PathPart extractLastPathPart(List<PathPart> pathParts) {
        return pathParts.get(pathParts.size() - 1);
    }

    public static PathPart extractLastPathPartAndModifyList(List<PathPart> pathParts) {
        return pathParts.remove(pathParts.size() - 1);
    }

    /**
     * Given a list of path parts, convert this to the list of contexts you would need
     * to retrieve the JSON path represented by the list of path parts.
     *
     * @param pathParts pathParts list to convert.
     * @return An array of contexts (CTXs).
     */
    public static CTX[] pathPartsToContextArray(List<PathPart> pathParts) {
        return pathParts.stream()
                .map(PathPart::toAerospikeContext)
                .toArray(CTX[]::new);
    }

    /**
     * Parse a JSON path string into a {@link JsonPathObject} object.
     *
     * @param jsonString the given JSON path string.
     * @return the {@link JsonPathObject} object.
     * @throws DocumentApiException.JsonParseException if fails to parse the JSON path string.
     */
    public JsonPathObject parse(String jsonString) throws DocumentApiException.JsonParseException {
        validateJsonPath(jsonString);

        StringTokenizer tokenizer = new StringTokenizer(jsonString, JSON_PATH_SEPARATOR);
        Integer index = getFirstIndexOfAQueryIndication(jsonString);
        // Query is required
        if (index != null) {
            /*
            Split the jsonString into 2 parts:
                1. A string of path parts before a query operator to fetch the smallest Json possible from Aerospike.
                2. A string of the remaining JsonPath to later use for executing a JsonPath query on the fetched Json
                   from Aerospike.

            For example:
            $.store.book[*].author
            store.book will be fetched from Aerospike and a JsonPath book[*].author query will be executed on the
            fetched results from Aerospike (key = book, value = nested Json).
            */
            jsonPathObject.setRequiresJsonPathQuery(true);
            String aerospikePathPartsString = jsonString.substring(0, index);
            String jsonPathPathPartsString = jsonString.substring(index);
            jsonPathObject.setJsonPathSecondStepQuery(jsonPathPathPartsString);
            tokenizer = new StringTokenizer(aerospikePathPartsString, JSON_PATH_SEPARATOR);
            if (!tokenizer.nextToken().equals(DOCUMENT_ROOT_TOKEN)) {
                throw new DocumentApiException.JsonPrefixException(jsonString);
            }
        }

        while (tokenizer.hasMoreTokens()) {
            parsePathPart(tokenizer.nextToken());
        }

        return jsonPathObject;
    }

    /**
     * An internal method to process the individual path parts.
     * Appends the found path parts to the list of path parts already found.
     * Expected form of pathPart is key[index1][index2].
     *
     * @param pathPart pathPart to parse.
     * @throws DocumentApiException.JsonPathException if fails to parse the pathPart string.
     */
    private void parsePathPart(String pathPart) throws DocumentApiException.JsonPathException {
        Matcher keyMatcher = PATH_PATTERN.matcher(pathPart);
        if ((!pathPart.contains("[")) && (!pathPart.contains("]"))) {
            // ignore * wildcard after a dot, it's the same as ending with a .path
            if (!pathPart.equals("*") && !pathPart.equals(DOCUMENT_ROOT_TOKEN)) {
                jsonPathObject.addPathPart(new MapPathPart(pathPart));
            }
        } else if (keyMatcher.find()) {
            String key = keyMatcher.group(1);
            if (!key.equals(DOCUMENT_ROOT_TOKEN)) {
                jsonPathObject.addPathPart(new MapPathPart(key));
            }
            Matcher indexMatcher = INDEX_PATTERN.matcher(pathPart);

            while (indexMatcher.find()) {
                jsonPathObject.addPathPart(new ListPathPart(Integer.parseInt(indexMatcher.group(2))));
            }
        } else {
            throw new DocumentApiException.JsonPathException(pathPart);
        }
    }

    private Integer getFirstIndexOfAQueryIndication(String jsonPath) {
        return jsonPathQueryIndications.stream()
                .map(jsonPath::indexOf)
                .filter(index -> index > 0)
                .min(Integer::compare)
                .orElse(null); // in case there no match for a query indication
    }

    private void validateJsonPath(String jsonString) throws DocumentApiException.JsonParseException {
        if (!jsonString.equals(DOCUMENT_ROOT_TOKEN)
                && !jsonString.startsWith("$.")
                && !jsonString.startsWith("$[")) {
            throw new DocumentApiException.JsonPrefixException(jsonString);
        }
    }
}
