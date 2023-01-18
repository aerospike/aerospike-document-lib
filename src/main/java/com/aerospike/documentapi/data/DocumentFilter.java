package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;

public interface DocumentFilter {

    public Filter toFilter();

    public Exp toFilterExpression();
}
