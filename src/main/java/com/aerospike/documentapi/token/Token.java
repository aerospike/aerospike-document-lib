package com.aerospike.documentapi.token;

public abstract class Token {

    private String string;
    private String queryConcatString;

    public String getString() {
        return string;
    }

    protected void setString(String string) {
        this.string = string;
        this.queryConcatString = string;
    }

    public String getQueryConcatString() {
        return queryConcatString;
    }

    protected void setQueryConcatString(String queryConcatString) {
        this.queryConcatString = queryConcatString;
    }

    public abstract TokenType getType();

    public boolean requiresJsonQuery() {
        return false;
    }
}
