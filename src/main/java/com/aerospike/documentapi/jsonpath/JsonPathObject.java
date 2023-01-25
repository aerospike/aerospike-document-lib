package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.token.ContextAwareToken;
import com.aerospike.documentapi.token.Token;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOT;

public class JsonPathObject {

    private final List<ContextAwareToken> notQueryTokens;
    private final List<Token> queryTokens;
    private String jsonPathSecondStepQuery = "";
    private boolean requiresJsonPathQuery;

    public JsonPathObject() {
        notQueryTokens = new ArrayList<>();
        queryTokens = new ArrayList<>();
    }

    public JsonPathObject copy() {
        String newJsonPathSecondStepQuery = getJsonPathSecondStepQuery();
        boolean newRequiresJsonPathQuery = requiresJsonPathQuery();

        JsonPathObject newJsonPathObject = new JsonPathObject();
        for (Token token : notQueryTokens) {
            newJsonPathObject.addToken(token);
        }
        newJsonPathObject.setJsonPathSecondStepQuery(newJsonPathSecondStepQuery);
        newJsonPathObject.setRequiresJsonPathQuery(newRequiresJsonPathQuery);
        return newJsonPathObject;
    }

    public List<ContextAwareToken> getTokensNotRequiringSecondStepQuery() {
        return notQueryTokens;
    }

    public List<Token> getTokensRequiringSecondStepQuery() {
        return queryTokens;
    }

    public void addToken(Token token) {
        // other tokens get here as well after class level requiresJsonPathQuery switches to true
        if (token.requiresJsonQuery() || this.requiresJsonPathQuery) {
            addQueryToken(token);
            setRequiresJsonPathQuery(true);
        } else {
            addPathToken((ContextAwareToken) token);
        }
    }

    public void addQueryToken(Token token) {
        queryTokens.add(token);
        appendToJsonPathQuery(token);
    }

    public void addPathToken(ContextAwareToken token) {
        notQueryTokens.add(token);
    }

    public boolean requiresJsonPathQuery() {
        return requiresJsonPathQuery;
    }

    public void setRequiresJsonPathQuery(boolean status) {
        requiresJsonPathQuery = status;
    }

    public String getJsonPathSecondStepQuery() {
        return jsonPathSecondStepQuery;
    }

    public void setJsonPathSecondStepQuery(String jsonPathSecondStepQuery) {
        this.jsonPathSecondStepQuery = jsonPathSecondStepQuery;
    }

    public void appendToJsonPathQuery(Token queryToken) {
        String tokenString = queryToken.getQueryConcatString();
        switch (queryToken.getType()) {
            case FUNCTION:
            case WILDCARD:
                jsonPathSecondStepQuery += DOT + tokenString;
                break;
            case LIST:
            case LIST_WILDCARD:
            case SCAN:
            case FILTER:
                jsonPathSecondStepQuery += tokenString;
                break;
            case MAP:
            default:
                jsonPathSecondStepQuery += jsonPathSecondStepQuery.isEmpty()
                        ? tokenString
                        : DOT + tokenString;
                break;
        }
    }
}
