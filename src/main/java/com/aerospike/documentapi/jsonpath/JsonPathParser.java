package com.aerospike.documentapi.jsonpath;

import com.aerospike.client.cdt.CTX;
import com.aerospike.documentapi.DocumentApiException;
import com.aerospike.documentapi.token.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Utility class for parsing JSONPath strings.
 */
public class JsonPathParser {

    static final String DOCUMENT_ROOT_TOKEN = "$";
    public static final String DEEP_SCAN = "..";

    // Paths should match this pattern i.e. key[index1][index2]...
    static final Pattern PATH_PATTERN = Pattern.compile("^([^\\[^\\]]*)(\\[(\\d+)\\])*$");
    // This pattern to extract index1,index2 ...
    static final Pattern INDEX_PATTERN = Pattern.compile("(\\[(\\d+)\\])");

    public static final List<String> functionIndication = Arrays.asList(
            "min()",
            "max()",
            "avg()",
            "stddev()",
            "length()",
            "sum()",
            "keys()",
            "first()",
            "last()"
    );
    public static final String FILTER_START_INDICATION = "[?(";
    public static final String FILTER_END_INDICATION = ")]";
    public static final char DOC_ROOT = '$';
    public static final char DOT = '.';
    public static final char OPEN_BRACKET = '[';
    public static final char CLOSE_BRACKET = ']';
    public static final char WILDCARD = '*';

    // Store representation of json path tokens
    private final JsonPathObject jsonPathObject;

    public JsonPathParser() {
        this.jsonPathObject = new JsonPathObject();
    }

    public static ContextAwareToken extractLastPathPart(List<ContextAwareToken> tokens) {
        return tokens.get(tokens.size() - 1);
    }

    public static ContextAwareToken extractLastPathPartAndModifyList(List<ContextAwareToken> tokens) {
        return tokens.remove(tokens.size() - 1);
    }

    /**
     * Given a list of tokens, convert this to the list of contexts you would need
     * to retrieve the JSON path represented by the list of tokens.
     *
     * @param tokens tokens list to convert.
     * @return an array of contexts (CTXs).
     */
    public static CTX[] pathTokensToContextArray(List<ContextAwareToken> tokens) {
        return tokens.stream()
                .map(ContextAwareToken::toAerospikeContext)
                .toArray(CTX[]::new);
    }

    /**
     * Parse a JSON path string into a {@link JsonPathObject} object.
     *
     * @param jsonString the given JSON path string.
     * @return the {@link JsonPathObject} object.
     * @throws DocumentApiException if fails to parse the JSON path string.
     */
    public JsonPathObject parse(String jsonString) {
        jsonString = jsonString.trim();
        validateJsonPathPrefix(jsonString);

        List<String> pathSplit = Arrays.asList(
                jsonString.split(Pattern.quote(String.valueOf(DOT)), -1)
        );
        validatePathSplit(jsonString, pathSplit);
        pathSplit = Collections.unmodifiableList(combineFilterParts(pathSplit));

        List<Token> prev = null;
        for (String pathPart : pathSplit) {
            List<Token> curr = parseToken(pathPart);
            if (skipIteration(curr, prev)) {
                prev = curr;
                continue;
            }
            curr.forEach(jsonPathObject::addToken);
            prev = curr;
        }
        return jsonPathObject;
    }

    private List<String> combineFilterParts(List<String> pathList) {
        List<String> newList = new ArrayList<>();
        String prev = null;
        String filter = "", preFilter = "";
        boolean filterProcStarted = false;
        for (int i = 0; i < pathList.size(); i++) {
            String curr = pathList.get(i);
            if (prev != null && curr != null && !curr.isEmpty()
                    && prev.contains(FILTER_START_INDICATION)
                    && !prev.contains(String.valueOf(CLOSE_BRACKET)) // has "[?" and not "]"
            ) {
                if (!filterProcStarted) {
                    int filterBeginningIdx = prev.indexOf(FILTER_START_INDICATION);
                    if (filterBeginningIdx > 0) {
                        preFilter = prev.substring(0, filterBeginningIdx);
                        filter = prev.substring(filterBeginningIdx);
                        newList.set(i - 1, preFilter);
                    }
                    filterProcStarted = true;
                }

                if (curr.contains(FILTER_END_INDICATION)) {
                    filter += DOT + curr;
                    newList.add(filter); // combined filter value between "[?" and "]"
                    prev = curr;
                    continue;
                } else {
                    filter += DOT + curr;
                    continue;
                }
            }
            prev = curr;
            newList.add(curr);
        }

        return newList;
    }

    private boolean skipIteration(List<Token> curr, List<Token> prev) {
        boolean res = false;

        // if path ends with a map wildcard after a map or a list element like $.example.*, $.example[10].* or $.*
        res = (curr != null && prev != null
                && curr.size() == 1 && prev.size() == 1
                && curr.get(0).getType() == TokenType.WILDCARD
                && curr.get(0).getString().charAt(0) == WILDCARD // "*", not "[*]"
                && (prev.get(0).getType() == TokenType.MAP || prev.get(0).getType() == TokenType.LIST
                || prev.get(0).getType() == TokenType.ROOT))
                // if current element is root
                || (curr != null && curr.size() == 1 && curr.get(0).getType() == TokenType.ROOT)
        ;

        return res;
    }

    private void validatePathSplit(String jsonPath, List<String> jsonPathSplit) {
        Iterator<String> iter = jsonPathSplit.listIterator();
        String prev = null, next = null;
        while (iter.hasNext()) {
            next = iter.next();
            if (next.equals("")) {
                if ("".equals(prev)) {
                    // path either ends with 2 or more dots or has 3 or more dots in the middle
                    throw new DocumentApiException.JsonPathException(jsonPath);
                }
                prev = next;
            } else {
                prev = null;
            }
        }
    }

    private List<Token> parseToken(String token) {
        // the initial string has been split between '.' and validated
        if (token.length() == 0) {
            token = DEEP_SCAN;
            Optional<Token> optional = ScanToken.match(token);
            return tokensListOrFail(optional, token);
        }

        switch (token.charAt(token.length() - 1)) {
            case DOC_ROOT:
                Optional<Token> optional = RootToken.match(token);
                return tokensListOrFail(optional, token);
            case CLOSE_BRACKET:
                return processPartWithBracketsOrFail(token);
            case WILDCARD:
                optional = WildcardToken.match(token);
                return tokensListOrFail(optional, token);
            default:
                return processDefaultOrFail(token);
        }
    }

    private List<Token> processDefaultOrFail(String token) {
        List<Token> list = processToken(
                Arrays.asList(
                        MapToken.match(token),
                        FunctionToken.match(token),
                        FilterToken.match(token)
                )
        );

        if (list != null && !list.isEmpty()) {
            return list;
        } else {
            throw new DocumentApiException.JsonPathException(token);
        }
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private List<Token> tokensListOrFail(Optional<Token> tokenOpt, String token) {
        if (tokenOpt.isPresent()) {
            return Collections.singletonList(tokenOpt.get());
        } else {
            throw new DocumentApiException.JsonPathException(token);
        }
    }

    private List<Token> processPartWithBracketsOrFail(String token) {
        List<Token> list = processToken(
                Arrays.asList(
                        WildcardToken.match(token),
                        FilterToken.match(token)
                )
        );
        if (list != null && !list.isEmpty()) {
            return list;
        } else {
            list = ListToken.parseToList(token);
        }
        if (list.size() == 0) {
            throw new DocumentApiException.JsonPathException(token);
        } else {
            return list;
        }
    }

    private List<Token> processToken(List<Optional<Token>> list) {
        List<Token> resList = null;

        for (Optional<Token> tokenOpt : list) {
            if (tokenOpt.isPresent()) {
                resList = Collections.singletonList(tokenOpt.get());
            }
        }

        return resList;
    }

    private void validateJsonPathPrefix(String jsonPath) {
        if (!jsonPath.equals(DOCUMENT_ROOT_TOKEN)
                && !jsonPath.startsWith("$.")
                && !jsonPath.startsWith("$[")) {
            throw new DocumentApiException.JsonPrefixException(jsonPath);
        }
    }
}
