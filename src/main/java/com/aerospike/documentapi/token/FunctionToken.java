package com.aerospike.documentapi.token;

import com.aerospike.documentapi.jsonpath.JsonPathParser;

import java.util.Optional;

public class FunctionToken extends Token {
    public FunctionToken(String strPart) {
        if (JsonPathParser.functionIndication.stream().noneMatch(strPart::contains))
            throw new IllegalArgumentException();

        setString(strPart);
    }

    public static Optional<Token> match(String strPart) {
        Token token = null;
        try {
            token = new FunctionToken(strPart);
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.of(token);
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
