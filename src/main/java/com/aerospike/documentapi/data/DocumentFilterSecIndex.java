package com.aerospike.documentapi.data;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;

public interface DocumentFilterSecIndex extends DocumentFilter {
    Filter toSecIndexFilter();

    void setIdxCollectionType(IndexCollectionType idxCollectionType);
}
