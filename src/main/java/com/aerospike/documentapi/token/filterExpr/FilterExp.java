package com.aerospike.documentapi.token.filterExpr;

import com.aerospike.documentapi.token.ContextAwareToken;
import com.aerospike.documentapi.token.Token;

import java.util.List;

public abstract class FilterExp {

    Type type;
    List<Token> tokens;

    public void setType(Type type) {
        this.type = type;
    }

    public void setTokens(List<Token> tokens) {
        this.tokens = tokens;
    }

    public Type getType() {
        return type;
    }
    public List<Token> getTokens() {
        return tokens;
    }

    enum Type {
        FILTER_OPERAND, FILTER_CRITERION;
    }

    public abstract Object[] getValues();
}
