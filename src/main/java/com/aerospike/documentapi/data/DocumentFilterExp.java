package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.documentapi.token.filterExpr.Operator;
import com.aerospike.documentapi.util.DocumentExp;

public class DocumentFilterExp implements DocumentFilter {
    
    private Exp exp;

    public DocumentFilterExp(String jsonPath, Operator.Simple operator, String binName, Object value) {
        switch (operator) {

            case LT:
                exp = DocumentExp.lt(binName, jsonPath, value);
                break;
            case GT:
                exp = DocumentExp.gt(binName, jsonPath, value);
                break;
            case LTE:
                exp = DocumentExp.le(binName, jsonPath, value);
                break;
            case GTE:
                exp = DocumentExp.ge(binName, jsonPath, value);
                break;
            case EQ:
                exp = DocumentExp.eq(binName, jsonPath, value);
                break;
            case NE:
                exp = DocumentExp.ne(binName, jsonPath, value);
                break;
            case TSEQ:
                break;
            case TSNE:
                break;
        }
    }

    @Override
    public Filter toFilter() {
        return null;
    }

    @Override
    public Exp toFilterExpression() {
        return exp;
    }
}
