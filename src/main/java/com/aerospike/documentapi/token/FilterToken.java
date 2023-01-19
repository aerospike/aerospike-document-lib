package com.aerospike.documentapi.token;

import com.aerospike.documentapi.jsonpath.JsonPathParser;

import java.util.Optional;

public class FilterToken extends Token {

    public FilterToken(String strPart) {
        if (!strPart.contains(JsonPathParser.FILTER_START_INDICATION)) throw new IllegalArgumentException();
        setString(strPart);
    }

    public static Optional<Token> match(String strPart) {
        Token token;
        try {
            token = new FilterToken(strPart);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }

    @Override
    public TokenType getType() {
        return TokenType.FILTER;
    }
}
