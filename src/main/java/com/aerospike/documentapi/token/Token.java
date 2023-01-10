package com.aerospike.documentapi.token;

// extenders except RootToken are to have public static boolean match(String, TokenAppender)
public abstract class Token {

    private String string;
    private String queryConcatString;

    protected void setString(String string) {
        this.string = string;
        this.queryConcatString = string;
    };

    protected void setQueryConcatString(String queryConcatString) {
        this.queryConcatString = queryConcatString;
    }

    public String getString() {
        return string;
    }

    public String getQueryConcatString() {
        return queryConcatString;
    }

    abstract boolean read(String strPart);

    public abstract TokenType getType();

    public abstract boolean requiresJsonQuery();
}
