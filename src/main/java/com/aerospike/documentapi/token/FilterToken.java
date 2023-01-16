package com.aerospike.documentapi.token;

import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.token.filterExpr.FilterExp;
import com.aerospike.documentapi.token.filterExpr.FilterExprFactory;
import com.aerospike.documentapi.token.filterExpr.FilterExprParser;
import com.aerospike.documentapi.token.filterExpr.Operator;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.FILTER_END_INDICATION;
import static com.aerospike.documentapi.token.filterExpr.Operator.isSpecial;

public class FilterToken extends Token {

    boolean requires2Step = true;

    @Override
    boolean read(String strPart) {
        if (!strPart.contains(JsonPathParser.FILTER_START_INDICATION)) return false;
        setString(strPart);

        int filterStartIdx = strPart.indexOf(JsonPathParser.FILTER_START_INDICATION);
        int filterEndIdx = strPart.length();
        if (!strPart.endsWith(FILTER_END_INDICATION)) {
            // if there might be a list elem after a filter
            filterEndIdx = strPart.indexOf(JsonPathParser.FILTER_END_INDICATION) + 2;
        }
        String filterExpr = strPart.substring(filterStartIdx + 2, filterEndIdx - 1).trim();
        FilterExp filterCriteria = FilterExprParser.parse(filterExpr);
        setFilterCriteria(filterCriteria);
//        Operator.OperatorType opType = ((FilterExprFactory.FilterCriterion) filterCriteria).getOperatorType();
//        requires2Step = (isSpecial(opType)); // temp, TBD
        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new FilterToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
    }

    @Override
    public boolean requiresJsonQuery() {
        return requires2Step;
    }

    @Override
    public TokenType getType() {
        return TokenType.FILTER;
    }
}
