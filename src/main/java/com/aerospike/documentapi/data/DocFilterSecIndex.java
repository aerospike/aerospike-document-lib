package com.aerospike.documentapi.data;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.documentapi.util.FilterConverter;

import static com.aerospike.client.query.IndexCollectionType.DEFAULT;

public class DocFilterSecIndex implements DocumentFilterSecIndex {

    private final String binName;
    private final String jsonPath;
    private final Operator operator;
    private final Object value;
    private IndexCollectionType idxCollectionType = DEFAULT;

    public DocFilterSecIndex(String binName, String jsonPath, Operator operator, Object value) {
        this.binName = binName;
        this.jsonPath = jsonPath;
        this.operator = operator;
        this.value = value;
    }

    public Filter toSecIndexFilter() {
        switch (operator) {
            case LT:
                return FilterConverter.lt(binName, jsonPath, value, idxCollectionType);
            case GT:
                return FilterConverter.gt(binName, jsonPath, value, idxCollectionType);
            case LE:
                return FilterConverter.le(binName, jsonPath, value, idxCollectionType);
            case GE:
                return FilterConverter.ge(binName, jsonPath, value, idxCollectionType);
            case EQ:
                return FilterConverter.eq(binName, jsonPath, value);
            case NE:
                throw new UnsupportedOperationException("'Not equals' secondary filter is not supported");
            default:
                return null;
        }
    }

    @Override
    public void setIdxCollectionType(IndexCollectionType idxCollectionType) {
        this.idxCollectionType = idxCollectionType;
    }
}
