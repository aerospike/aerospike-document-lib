package com.aerospike.documentapi.data;

public enum Operator {

    GTE(">="),
    LTE("<="),
    EQ("=="),
    NE("!="),
    LT("<"),
    GT(">"),
    REGEX("=~");
//    AND("&&"),
//    OR("||"),
//    NOT_EXISTS("!"),
//    EXISTS("EXISTS"),
//    TSEQ("==="), // Type safe equals
//    TSNE("!=="), // Type safe not equals
//    NIN("NIN"),
//    IN("IN"),
//    CONTAINS("CONTAINS"),
//    ALL("ALL"),
//    SIZE("SIZE"),
//    TYPE("TYPE"),
//    MATCHES("MATCHES"),
//    EMPTY("EMPTY"),
//    SUBSETOF("SUBSETOF"),
//    ANYOF("ANYOF"),
//    NONEOF("NONEOF");

    private final String name;

    Operator(String operatorName) {
        name = operatorName;
    }

    public static Operator fromString(String name) {
        for (Operator v : Operator.values()) {
            if (v.name.equalsIgnoreCase(name)) {
                return v;
            }
        }
        return null;
    }

    public String getName() {
        return this.name;
    }
}
