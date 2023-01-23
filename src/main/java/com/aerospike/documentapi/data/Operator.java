package com.aerospike.documentapi.data;

public enum Operator {

    EQ("=="),
    NE("!="),
    LT("<"),
    GT(">"),
    GE(">="),
    LE("<="),
    REGEX("=~");

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
