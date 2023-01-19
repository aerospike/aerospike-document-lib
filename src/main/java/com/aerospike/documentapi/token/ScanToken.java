package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DEEP_SCAN;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOT;

public class ScanToken extends Token {

    public ScanToken(String strPart) {
        if (!DEEP_SCAN.equals(strPart)) throw new IllegalArgumentException();
        setString(strPart);

        // a dot is added during concatenation
        setQueryConcatString(String.valueOf(DOT));
    }

    public static Optional<Token> match(String strPart) {
        Token token = null;
        try {
            token = new ScanToken(strPart);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    @Override
    public TokenType getType() {
        return TokenType.SCAN;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }
}
