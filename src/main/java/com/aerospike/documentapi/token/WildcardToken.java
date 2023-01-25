package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.CLOSE_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.OPEN_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.WILDCARD;
import static com.aerospike.documentapi.token.TokenType.LIST_WILDCARD;

public class WildcardToken extends Token {

    private boolean isInList;

    public WildcardToken(String strPart) {
        if (!String.valueOf(WILDCARD).equals(strPart))
            throw new IllegalArgumentException();
        setString(strPart);
    }

    public WildcardToken(String strPart, boolean inList) {
        String WILDCARD_LIST_ELEM = OPEN_BRACKET + String.valueOf(WILDCARD) + CLOSE_BRACKET;
        if (!String.valueOf(WILDCARD).equals(strPart) && !WILDCARD_LIST_ELEM.equals(strPart))
            throw new IllegalArgumentException();
        if (inList) {
            setString(WILDCARD_LIST_ELEM);
            isInList = true;
        } else {
            setString(strPart);
        }
    }

    public static Optional<Token> match(String strPart) {
        Token token;
        try {
            token = new WildcardToken(strPart);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    @Override
    public TokenType getType() {
        return isInList ? LIST_WILDCARD : TokenType.WILDCARD;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }
}
