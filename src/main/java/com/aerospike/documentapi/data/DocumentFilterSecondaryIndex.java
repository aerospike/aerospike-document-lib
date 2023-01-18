package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

public class DocumentFilterSecondaryIndex implements DocumentFilter {
    @Override
    public Filter toFilter() {
        return null;
    }

    @Override
    public Exp toFilterExpression() {
        return null;
    }
}
