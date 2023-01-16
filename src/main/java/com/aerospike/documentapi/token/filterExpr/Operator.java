package com.aerospike.documentapi.token.filterExpr;

import java.util.Arrays;

public class Operator {

    private Operator() {
    }

    public static boolean isSimple(String op) {
        return Arrays.stream(Simple.values()).anyMatch((t) -> t.getName().equalsIgnoreCase(op));
    }

    public static boolean isLogic(String op) {
        return Arrays.stream(Logic.values()).anyMatch((t) -> t.getName().equalsIgnoreCase(op));
    }

    public static boolean isLogicUnary(String op) {
        return Arrays.stream(LogicUnary.values()).anyMatch((t) -> t.getName().equalsIgnoreCase(op));
    }

    public static boolean isSpecial(String op) {
        return Arrays.stream(Special.values()).anyMatch((t) -> t.name().equalsIgnoreCase(op));
    }

    public static boolean isSpecial(OperatorType op) {
        return isSpecial(op.toString());
    }

    public interface OperatorType {}

    public enum Simple implements OperatorType {
        GTE(">="),
        LTE("<="),
        EQ("=="),
        TSEQ("==="), // Type safe equals
        NE("!="),
        TSNE("!=="), // Type safe not equals
        LT("<"),
        GT(">");

        private final String name;

        Simple(String op) {
            name = op;
        }

        public String getName() {
            return this.name;
        }

        public static Simple fromString(String name) {
            for (Simple v : Simple.values()) {
                if (v.name.equalsIgnoreCase(name)) {
                    return v;
                }
            }
            return null;
        }
    }

    public enum Logic implements OperatorType {
        AND("&&"), OR("||");

        private final String name;

        Logic(String op) {
            name = op;
        }

        public String getName() {
            return this.name;
        }

        public static Logic fromString(String name) {
            for (Logic v : Logic.values()) {
                if (v.name.equalsIgnoreCase(name)) {
                    return v;
                }
            }
            return null;
        }
    }

    public enum LogicUnary implements OperatorType {
        NOT_EXISTS("!"), EXISTS("EXISTS");

        private final String name;

        LogicUnary(String op) {
            name = op;
        }

        public String getName() {
            return this.name;
        }

        public static LogicUnary fromString(String name) {
            for (LogicUnary v : LogicUnary.values()) {
                if (v.name.equalsIgnoreCase(name)) {
                    return v;
                }
            }
            return null;
        }
    }

    public enum Special implements OperatorType {
        REGEX("=~"),
        NIN("NIN"),
        IN("IN"),
        CONTAINS("CONTAINS"),
        ALL("ALL"),
        SIZE("SIZE"),
        TYPE("TYPE"),
        MATCHES("MATCHES"),
        EMPTY("EMPTY"), // TODO: followed by true/false?
        SUBSETOF("SUBSETOF"),
        ANYOF("ANYOF"),
        NONEOF("NONEOF");


        private final String name;

        Special(String op) {
            name = op;
        }

        public String getName() {
            return this.name;
        }

        public static Special fromString(String name) {
            for (Special v : Special.values()) {
                if (v.name.equalsIgnoreCase(name)) {
                    return v;
                }
            }
            return null;
        }
    }
}
