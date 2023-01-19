package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOC_ROOT;

public class RootToken extends Token {

    public RootToken(String strPart) {
        if (!String.valueOf(DOC_ROOT).equals(strPart)) throw new IllegalArgumentException();
        setString(strPart);
    }

    public static Optional<Token> match(String strPart) {
        Token token;
        try {
            token = new RootToken(strPart);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.of(token);
    }

    @Override
    public TokenType getType() {
        return TokenType.ROOT;
    }
}
