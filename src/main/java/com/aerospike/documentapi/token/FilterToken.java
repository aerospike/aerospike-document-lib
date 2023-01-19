package com.aerospike.documentapi.token;

import com.aerospike.documentapi.jsonpath.JsonPathParser;

import java.util.Optional;

public class FilterToken extends Token {

    @Override
    boolean read(String strPart) {
        if (!strPart.contains(JsonPathParser.FILTER_START_INDICATION)) return false;
        setString(strPart);

        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new FilterToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
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
