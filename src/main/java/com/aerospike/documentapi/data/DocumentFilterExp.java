package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;

public interface DocumentFilterExp extends DocumentFilter {
    Exp toFilterExp();

    void setRegexFlags(int regexFlags);
}
