package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DEEP_SCAN;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOT;

public class ScanToken extends Token {

    @Override
    public boolean read(String strPart) {
        if (!DEEP_SCAN.equals(strPart)) return false;

        setString(strPart);
        // a dot is added during concatenation to a query string
        setQueryConcatString(String.valueOf(DOT));
        return true;
    }

    @Override
    public TokenType getType() {
        return TokenType.SCAN;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new ScanToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
    }
}
