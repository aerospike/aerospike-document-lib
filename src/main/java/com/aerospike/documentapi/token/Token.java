package com.aerospike.documentapi.token;

import com.aerospike.documentapi.token.filterExpr.FilterExp;

public abstract class Token {

    private String string;
    private String queryConcatString;
    private FilterExp filterCriteria = null;

    protected void setString(String string) {
        this.string = string;
        this.queryConcatString = string;
    };

    public void setFilterCriteria(FilterExp filterCriteria) {
        this.filterCriteria = filterCriteria;
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

    abstract boolean read(String strPart);

    public abstract TokenType getType();

    public boolean requiresJsonQuery() {
        return false;
    };

    public FilterExp getFilterCriteria() {
        return filterCriteria;
    }
}
