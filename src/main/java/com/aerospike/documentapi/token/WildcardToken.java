package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.CLOSE_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.OPEN_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.WILDCARD;

public class WildcardToken extends Token {

    private final String LIST_WILDCARD = OPEN_BRACKET + String.valueOf(WILDCARD) + CLOSE_BRACKET;

    public WildcardToken(String strPart) {
        if (!String.valueOf(WILDCARD).equals(strPart) && !LIST_WILDCARD.equals(strPart))
            throw new IllegalArgumentException();
        setString(strPart);
    }

    public WildcardToken(String strPart, boolean inList) {
        if (!String.valueOf(WILDCARD).equals(strPart) && !LIST_WILDCARD.equals(strPart))
            throw new IllegalArgumentException();
        if (inList) setString(LIST_WILDCARD); else setString(strPart);
    }

    public static Optional<Token> match(String strPart) {
        Token token = null;
        try {
            token = new WildcardToken(strPart);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    @Override
    public TokenType getType() {
        return TokenType.WILDCARD;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }
}
