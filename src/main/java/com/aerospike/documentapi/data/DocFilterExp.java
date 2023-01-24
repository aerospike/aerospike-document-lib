package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;
import com.aerospike.documentapi.util.ExpConverter;

public class DocFilterExp implements DocumentFilterExp {

    private final String binName;
    private final String jsonPath;
    private final Operator operator;
    private final Object value;
    private Integer regexFlags = null;

    public DocFilterExp(String binName, String jsonPath, Operator operator, Object value) {
        this.binName = binName;
        this.jsonPath = jsonPath;
        this.operator = operator;
        this.value = value;
    }

    public Exp toFilterExp() {
        switch (operator) {
            case LT:
                return ExpConverter.lt(binName, jsonPath, value);
            case GT:
                return ExpConverter.gt(binName, jsonPath, value);
            case LE:
                return ExpConverter.le(binName, jsonPath, value);
            case GE:
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

    @Override
    public void setRegexFlags(int regexFlags) {
        this.regexFlags = regexFlags;
    }
}
