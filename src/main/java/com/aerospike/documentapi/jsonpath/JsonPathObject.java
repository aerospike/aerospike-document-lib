package com.aerospike.documentapi.jsonpath;

import com.aerospike.documentapi.token.Token;
import com.aerospike.documentapi.token.ContextAwareToken;
import com.aerospike.documentapi.token.TokenType;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOT;

public class JsonPathObject {

    private final List<ContextAwareToken> pathTokens;
    private final List<Token> queryTokens;
    private String jsonPathSecondStepQuery = "";
    private boolean requiresJsonPathQuery;

    public JsonPathObject() {
        pathTokens = new ArrayList<>();
        queryTokens = new ArrayList<>();
    }

    public JsonPathObject copy() {
        String newJsonPathSecondStepQuery = getJsonPathSecondStepQuery();
        boolean newRequiresJsonPathQuery = requiresJsonPathQuery();

        JsonPathObject newJsonPathObject = new JsonPathObject();
        for (Token token : pathTokens) {
            newJsonPathObject.addToken(token);
        }
        newJsonPathObject.setJsonPathSecondStepQuery(newJsonPathSecondStepQuery);
        newJsonPathObject.setRequiresJsonPathQuery(newRequiresJsonPathQuery);
        return newJsonPathObject;
    }

    public List<ContextAwareToken> getPathTokensWithoutQuery() {
        return pathTokens;
    }

    public List<Token> getQueryTokens() {
        return queryTokens;
    }

    public void addToken(Token token) {
        // pathTokens get here as well after setRequiresJsonPathQuery has been switched to true
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
        pathTokens.add(token);
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
        jsonPathSecondStepQuery += jsonPathSecondStepQuery.isEmpty()
                ? queryToken.getType() == TokenType.FUNCTION ? DOT + tokenString : tokenString
                : DOT + tokenString;
    }
}
