package com.aerospike.documentapi.token;

import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOC_ROOT;

public class RootToken extends Token {

    @Override
    public boolean read(String strPart) {
        if (!String.valueOf(DOC_ROOT).equals(strPart)) return false;

        setString(strPart);
        return true;
    }

    @Override
    public TokenType getType() {
        return TokenType.ROOT;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new RootToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
    }
}
