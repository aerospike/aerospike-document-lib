package com.aerospike.documentapi.data;

import com.aerospike.client.exp.Exp;
import com.aerospike.documentapi.util.DocumentExp;

public class DocumentFilterExp {
    
    private Exp exp;

    public DocumentFilterExp(String binName, String jsonPath, Operator.Simple  operator,Object value) {
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

    public Exp toFilterExpression() {
        return exp;
    }
}
