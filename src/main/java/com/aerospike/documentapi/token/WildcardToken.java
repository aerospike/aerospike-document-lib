package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.CLOSE_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.OPEN_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.WILDCARD;

public class WildcardToken extends Token {

    private final String LIST_WILDCARD = OPEN_BRACKET + String.valueOf(WILDCARD) + CLOSE_BRACKET;

    @Override
    public boolean read(String strPart) {
        if (!String.valueOf(WILDCARD).equals(strPart) && !LIST_WILDCARD.equals(strPart)) return false;

        setString(strPart);
        return true;
    }

    // specifically setting LIST_WILDCARD via setString() even if '*' has been given
    public boolean read(String strPart, boolean inList) {
        if (read(strPart) && inList) {
            setString(LIST_WILDCARD);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public TokenType getType() {
        return TokenType.WILDCARD;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new WildcardToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
    }
}
