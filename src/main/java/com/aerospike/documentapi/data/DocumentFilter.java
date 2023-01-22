package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.documentapi.util.ExpConverter;
import com.aerospike.documentapi.util.FilterConverter;
import lombok.Setter;

import static com.aerospike.client.query.IndexCollectionType.DEFAULT;

public class DocumentFilter {

    private final String binName;
    private final String jsonPath;
    private final Operator operator;
    private final Object value;
    @Setter
    private Integer regexFlags = null;
    @Setter
    private IndexCollectionType indexCollectionType = DEFAULT;

    public DocumentFilter(String binName, String jsonPath, Operator operator, Object value) {
        this.binName = binName;
        this.jsonPath = jsonPath;
        this.operator = operator;
        this.value = value;
    }

    public Exp toFilterExpression() {
        switch (operator) {
            case LT:
                return ExpConverter.lt(binName, jsonPath, value);
            case GT:
                return ExpConverter.gt(binName, jsonPath, value);
            case LTE:
                return ExpConverter.le(binName, jsonPath, value);
            case GTE:
                return ExpConverter.ge(binName, jsonPath, value);
            case EQ:
                return ExpConverter.eq(binName, jsonPath, value);
            case NE:
                return ExpConverter.ne(binName, jsonPath, value);
            case REGEX:
                return ExpConverter.regex(binName, jsonPath, value.toString(), regexFlags);
            default:
                return null;
        }
    }

    public Filter toSecIndexFilter() {
        switch (operator) {
            case LT:
                return FilterConverter.lt(binName, jsonPath, value, indexCollectionType);
            case GT:
                return FilterConverter.gt(binName, jsonPath, value, indexCollectionType);
            case LTE:
                return FilterConverter.lte(binName, jsonPath, value, indexCollectionType);
            case GTE:
                return FilterConverter.gte(binName, jsonPath, value, indexCollectionType);
            case EQ:
                return FilterConverter.eq(binName, jsonPath, value);
            case NE:
                return FilterConverter.ne(binName, jsonPath, value);
            default:
                return null;
        }
    }
}
