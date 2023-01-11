package com.aerospike.documentapi.token.filterCriteria;

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

    public enum Simple {
        EQUAL("=="), UNEQUAL("!="), GREATER(">"), GREATEREQ(">="), LESS("<"), LESSEQ("<=");

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

    public enum Logic {
//        AND("and"), OR("or");
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

    public enum LogicUnary {
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

    public enum Special {
        LAST_UPDATE, VOID_TIME, DIGEST_MODULO, STRING_REGEX, GEOJSON_WITHIN, GEOJSON_CONTAINS, LIST_ITERATE_OR, MAPKEY_ITERATE_OR, MAPVAL_ITERATE_OR, LIST_ITERATE_AND, MAPKEY_ITERATE_AND, MAPVAL_ITERATE_AND
    }
}
