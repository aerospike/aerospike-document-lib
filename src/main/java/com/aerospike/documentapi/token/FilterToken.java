package com.aerospike.documentapi.token;

import com.aerospike.client.exp.Exp;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.token.filterCriteria.FilterCriteria;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.filterEndIndication;

public class FilterToken extends Token {

    private Exp filterCriteria;

    @Override
    boolean read(String strPart) {
        if (!strPart.contains(JsonPathParser.filterStartIndication)) return false;
        setString(strPart);

        int filterStartIdx = strPart.indexOf(JsonPathParser.filterStartIndication);
        int filterEndIdx = strPart.length();
        if (!strPart.endsWith(filterEndIndication)) {
            filterEndIdx = strPart.indexOf(JsonPathParser.filterEndIndication) + 2; // TODO: can there be a list elem after a filter?
        }
        String filterExpr = strPart.substring(filterStartIdx + 2, filterEndIdx - 1).trim(); // leaving parentheses
//        validate(criteria);// TODO
//        filterCriteria = FilterCriteria.parse(filterExpr);
        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new FilterToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
    }

    @Override
    public TokenType getType() {
        return TokenType.FILTER;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }
}
