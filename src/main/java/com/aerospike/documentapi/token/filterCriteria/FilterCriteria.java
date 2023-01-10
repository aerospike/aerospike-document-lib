package com.aerospike.documentapi.token.filterCriteria;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterCriteria {

    protected static final Pattern pattern = Pattern.compile("(\\))|(\\()|([^\\s)(]+)", Pattern.CASE_INSENSITIVE);

    protected static List<String> parseExpressionString(String expression) {
        Matcher matcher = pattern.matcher(expression);
        List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        return tokens;
    }

    protected static <U> boolean forAllEqual(Collection<U> list) {
        return new HashSet<>(list).size() <= 1;
    }

    /**
     * Parses a logical expression string to an Aerospike Filter Expression.
     *
     * @param expression the string representation of a logical expression.
     * @return the {@link Expression} for the specified logical expression.
     */
//        @Override
    public static Exp parse(String expression) {
        List<String> tokens = parseExpressionString(expression);
        Stack<NestedBlock> stack = new Stack<>();

        for (int i = 0; i < tokens.size(); i++) {
            String t = tokens.get(i).trim();
            if (t.equals("(")) {
                stack.push(new NestedBlock());
            } else if (t.equals(")")) {
                NestedBlock block = stack.pop();
                Exp exp = (Exp) block.filters.get(0);
                if (!block.logicOperators.isEmpty()) {
                    Preconditions.checkState(forAllEqual(block.logicOperators),
                            String.format("Only equal logic operations are required on the same level, given '%s'",
                                    asString(block.logicOperators)));
                    exp = FilterExpFactory.getLogicalExp(block.logicOperators.pop(), block.filters.toArray(new Exp[0]));
                }
                if (!stack.peek().unaryLogicOperators.isEmpty())
                    exp = FilterExpFactory.getUnaryLogicalExp(stack.peek().unaryLogicOperators.pop(), exp);
                stack.peek().filters.push(exp);
            } else if (Operator.isSimple(t)) {
                stack.peek().simpleOperators.push(t);
            } else if (Operator.isLogic(t)) {
                stack.peek().logicOperators.push(t);
            } else if (Operator.isLogicUnary(t)) {
                stack.peek().unaryLogicOperators.push(t);
            } else if (Operator.isSpecial(t)) {
                // read special operation
                StringBuilder buff = new StringBuilder();
                do {
                    buff.append(tokens.get(++i));
                } while (!tokens.get(i).equals(")"));
                Exp special = FilterExpFactory.getSpecialExp(t, buff.toString());
                if (!stack.peek().unaryLogicOperators.isEmpty())
                    special = FilterExpFactory.getUnaryLogicalExp(stack.peek().unaryLogicOperators.pop(), special);
                stack.peek().filters.push(special);
            } else {
                // is operand
                if (stack.peek().simpleOperators.size() > 0) {
                    if (stack.peek().operands.size() == 1) {
                        Exp exp = FilterExpFactory.getCompareExp(stack.peek().operands.pop(),
                                stack.peek().simpleOperators.pop(), t);
                        if (stack.peek().unaryLogicOperators.size() > 0)
                            exp = FilterExpFactory.getUnaryLogicalExp(stack.peek().unaryLogicOperators.pop(), exp);
                        stack.peek().filters.push(exp);
                    }
                } else {
                    stack.peek().operands.push(t);
                }
            }
        }

        NestedBlock block = stack.pop();
        Exp exp = block.filters.get(0);
        if (!block.logicOperators.isEmpty()) {
            Preconditions.checkState(forAllEqual(block.logicOperators),
                    String.format("Only equal logic operations are required on the same level, given '%s'",
                            asString(block.logicOperators)));
            exp = FilterExpFactory.getLogicalExp(block.logicOperators.peek(), block.filters.toArray(new Exp[0]));
        }

//        return Exp.build(exp);
        return exp;
    }

    private static String asString(Stack<String> stack) {
        return String.join(",", stack);
    }

    private static class NestedBlock {
        Stack<String> simpleOperators;
        Stack<String> logicOperators;
        Stack<String> unaryLogicOperators;
        Stack<String> operands;
        Stack<Exp> filters;

        NestedBlock() {
            simpleOperators = new Stack<>();
            logicOperators = new Stack<>();
            unaryLogicOperators = new Stack<>();
            operands = new Stack<>();
            filters = new Stack<Exp>();
        }
    }
}
