package com.aerospike.documentapi.token;

public abstract class Token {

    private String string;
    private String queryConcatString;

    protected void setString(String string) {
        this.string = string;
        this.queryConcatString = string;
    }

    protected void setQueryConcatString(String queryConcatString) {
        this.queryConcatString = queryConcatString;
    }

    public String getString() {
        return string;
    }

    public String getQueryConcatString() {
        return queryConcatString;
    }

    public abstract TokenType getType();

    public boolean requiresJsonQuery() {
        return false;
    }
}
