package com.aerospike.documentapi.token.filterExpr;

import java.util.Locale;

public enum RelationalOperator {

    GTE(">="),
    LTE("<="),
    EQ("=="),

    // Type safe equals
    TSEQ("==="),
    NE("!="),

    // Type safe not equals
    TSNE("!=="),
    LT("<"),
    GT(">"),
    REGEX("=~"),
    NIN("NIN"),
    IN("IN"),
    CONTAINS("CONTAINS"),
    ALL("ALL"),
    SIZE("SIZE"),
    EXISTS("EXISTS"),
    NOT_EXISTS("!@"),
    TYPE("TYPE"),
    MATCHES("MATCHES"),
    EMPTY("EMPTY"),
    SUBSETOF("SUBSETOF"),
    ANYOF("ANYOF"),
    NONEOF("NONEOF");

    private final String operatorStr;

    RelationalOperator(String operatorStr) {
        this.operatorStr = operatorStr;
    }

    public static RelationalOperator fromString(String operatorString) {
        String upperCaseOperatorString = operatorString.toUpperCase(Locale.ROOT);
        for (RelationalOperator operator : RelationalOperator.values()) {
            if (operator.operatorStr.equals(upperCaseOperatorString) ){
                return operator;
            }
        }
        throw new IllegalArgumentException(
                String.format("Cannot recognize filter operator '%s'", operatorString));
    }

    @Override
    public String toString() {
        return operatorStr;
    }
}
