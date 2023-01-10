package com.aerospike.documentapi.token;

import com.aerospike.documentapi.jsonpath.JsonPathParser;

import java.util.Optional;

public class FunctionToken extends Token {

    @Override
    boolean read(String strPart) {
        if (JsonPathParser.functionIndication.stream().noneMatch(strPart::contains)) return false;

        setString(strPart);
        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token = new FunctionToken();
        return token.read(strPart) ? Optional.of(token) : Optional.empty();
    }

    @Override
    public TokenType getType() {
        return TokenType.FUNCTION;
    }

    @Override
    public boolean requiresJsonQuery() {
        return true;
    }
}
