package com.aerospike.documentapi.token.filterExpr;

import com.aerospike.client.exp.Exp;
import com.aerospike.documentapi.token.Token;
import com.google.common.base.Preconditions;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Optional;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.CURR_ELEM;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.FILTER_START_INDICATION;
import static com.aerospike.documentapi.token.ListToken.parseToList;
import static com.aerospike.documentapi.token.filterExpr.Operator.LogicUnary.EXISTS;
import static com.aerospike.documentapi.token.filterExpr.Operator.LogicUnary.NOT_EXISTS;

public class FilterExprFactory {

    protected static Optional<Integer> parseInt(String s) {
        int i;
        try {
            i = Integer.parseInt(s);
        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.of(i);
    }

    public static FilterCriterion getUnaryLogicalCriterion(String logicalOp, String operand) {
        final Operator.LogicUnary op = Operator.LogicUnary.fromString(logicalOp);
        Preconditions.checkNotNull(op, "getUnaryLogicalExp operator fromString");
        FilterOperand part = new FilterOperand(operand);
        switch (op) {
            case NOT_EXISTS:
                return new FilterCriterion(NOT_EXISTS, part);
            case EXISTS:
                return new FilterCriterion(EXISTS, part);
            default:
                throw new InvalidParameterException(String.format("Invalid unary logical operator name: %s", logicalOp));
        }
    }

    public static FilterCriterion getCompareCriterion(String op, String left, String right) {
        return new FilterCriterion(
                toSimpleOperator(op),
                new FilterOperand(left),
                new FilterOperand(right)
        );
    }

    public static FilterCriterion getLogicalCriterion(String logicalOp, FilterCriterion[] criteria) {
        return new FilterCriterion(toLogicalOperator(logicalOp), criteria);
    }

    private static class FilterOperand extends FilterExp {

        private final Exp.Type type;
        private final Object value;
        private List<Token> tokens;

        private FilterOperand(String value) {
            Optional<Integer> intVal = parseInt(value);
            if (intVal.isPresent()) {
                this.type = Exp.Type.INT;
                this.value = intVal.get();
            } else {
                this.type = Exp.Type.STRING;
                this.value = value;
                setTokens(parseToList(value.substring(value.indexOf(CURR_ELEM + ".") + 2)));
            }

            setType(Type.FILTER_OPERAND);
        }

        public Exp.Type getOperandType() {
            return type;
        }

        @Override
        public Object[] getValues() {
            return new Object[]{value};
        }
    }

    public static class FilterCriterion extends FilterExp {

        private final Operator.OperatorType operatorType;
        private final FilterExp[] filterExpOperands;

        public FilterCriterion(Operator.OperatorType operatorType, FilterExp... filterExpOperands) {
            this.operatorType = operatorType;
            this.filterExpOperands = filterExpOperands;
            setType(Type.FILTER_CRITERION);
        }

        public Operator.OperatorType getOperatorType() {
            return operatorType;
        }

        @Override
        public Object[] getValues() {
            return filterExpOperands;
        }
    }

    private static Operator.OperatorType toSimpleOperator(String operator) {
        final Operator.Simple op = Operator.Simple.fromString(operator);
        Preconditions.checkNotNull(op, String.format("Cannot parse operator name '%s'", operator));
        return op;
    }

    private static Operator.OperatorType toLogicalOperator(String operator) {
        final Operator.Logic op = Operator.Logic.fromString(operator);
        Preconditions.checkNotNull(op, String.format("Cannot parse operator name '%s'", operator));
        return op;
    }
}
