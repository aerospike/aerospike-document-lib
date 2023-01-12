package com.aerospike.documentapi.token.filterExpr;

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

import static com.aerospike.documentapi.token.filterExpr.Operator.LogicUnary.EXISTS;
import static com.aerospike.documentapi.token.filterExpr.Operator.LogicUnary.NOT_EXISTS;

public class FilterExprParser {

    //    protected static final Pattern pattern = Pattern.compile("(\\))|(\\()|([^\\s)(]+)", Pattern.CASE_INSENSITIVE);
    protected static final Pattern pattern = Pattern.compile("(\\))|(\\()|([^)(<>=~!&|]+)|([<>=~!&|]+)",
            Pattern.CASE_INSENSITIVE);

    protected static List<String> parseExpressionString(String expression) {
        Matcher matcher = pattern.matcher(expression);
        List<String> tokens = new ArrayList<>();
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
        if (tokens.isEmpty()) {
            throw new RuntimeException(String.format("Unable to parse filter expression '%s'", expression));
        } else {
            return tokens;
        }
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
        stack.push(new NestedBlock());

        for (int i = 0; i < tokens.size(); i++) { // TODO =~
            String token = tokens.get(i).trim();
            if (token.equals("(")) {
                stack.push(new NestedBlock());
            } else if (token.equals(")")) {
                NestedBlock block = stack.pop();
                checkUnaryLogicalOpExp(block, stack); // TODO: build a NE/E Exp
                Exp exp = block.filters.get(0);
                if (exp == null) {
                    throw new RuntimeException("Empty block filters stack"); // TODO
                }
                if (!block.logicOperators.isEmpty()) {
                    Preconditions.checkState(forAllEqual(block.logicOperators),
                            String.format("Only equal logic operations are required on the same level, cannot process '%s'." +
                                            " Use parentheses to combine both logical operators if needed.",
                                    asString(block.logicOperators)));
                    exp = FilterExpFactory.getLogicalExp(block.logicOperators.pop(), block.filters.toArray(new Exp[0]));
                }
//                if (!stack.peek().unaryLogicOperators.isEmpty())
//                if (!block.unaryLogicOperators.isEmpty())
//                    exp = FilterExpFactory.getUnaryLogicalExp(stack.peek().unaryLogicOperators.pop(), exp);
                stack.peek().filters.push(exp);
            } else if (Operator.isSimple(token)) {
                stack.peek().simpleOperators.push(token);
            } else if (Operator.isLogic(token)) {
                stack.peek().logicOperators.push(token);
            } else if (Operator.isLogicUnary(token)) {
                stack.peek().unaryLogicOperators.push(token);
            } else if (Operator.isSpecial(token)) {
                // read special operation
                StringBuilder buff = new StringBuilder(); // TODO: check and edit
//                do {
//                    buff.append(tokens.get(++i));
//                } while (!tokens.get(i).equals(")"));
//                Exp special = FilterExpFactory.getSpecialExp(token, buff.toString()); // TODO: check and edit
//                if (!stack.peek().unaryLogicOperators.isEmpty())
//                    special = FilterExpFactory.getNotExistsLogicalExp(stack.peek().unaryLogicOperators.pop(), special);
//                stack.peek().filters.push(special);
            } else {
                // is operand
                if (stack.peek().simpleOperators.size() > 0) {
                    if (stack.peek().operands.size() == 1) {
//                        Exp exp = FilterExpFactory.getCompareExp(stack.peek().operands.pop(),
//                                stack.peek().simpleOperators.pop(), token);
                        Exp exp = null;
                        FilterExpFactory.FilterCriterion filterCriterion = FilterExpFactory.getFilterCriterion(
                                stack.peek().simpleOperators.pop(),
                                stack.peek().operands.pop(),
                                token
                        );
//                        if (stack.peek().unaryLogicOperators.size() > 0) { // TODO: check and edit
//                            exp = FilterExpFactory.getNotExistsLogicalExp(stack.peek().unaryLogicOperators.pop(), exp);
//                    }
                        stack.peek().filters.push(exp);
                    }
                } else {
                    stack.peek().operands.push(token);
                }
            }
        }

        NestedBlock block = stack.pop();
        Exp exp = block.filters.get(0);
        if (!block.logicOperators.isEmpty()) {
            Preconditions.checkState(forAllEqual(block.logicOperators),
                    String.format("Only equal logic operations are required on the same level, cannot process '%s'." +
                                    " Use parentheses to combine both logical operators if needed.",
                            asString(block.logicOperators)));
            exp = FilterExpFactory.getLogicalExp(block.logicOperators.peek(), block.filters.toArray(new Exp[0]));
        }

        return exp;
    }

    private static void checkUnaryLogicalOpExp(NestedBlock block, Stack<NestedBlock> stack) {
        if (block.operands.size() == 1
                && block.simpleOperators.isEmpty()
//                && block.filters.isEmpty()
                && block.unaryLogicOperators.isEmpty()
        ) {
            if (block.operands.peek().startsWith(NOT_EXISTS.toString())) {
                block.unaryLogicOperators.push(NOT_EXISTS.toString());
            } else {
                block.unaryLogicOperators.push(EXISTS.toString());
            }
            Exp exp = FilterExpFactory.getUnaryLogicalExp(block.unaryLogicOperators.pop(),
                    block.operands.pop());
            block.filters.push(exp);
        }
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
//        Stack<FilterCriterion> criteria;

        NestedBlock() {
            simpleOperators = new Stack<>();
            logicOperators = new Stack<>();
            unaryLogicOperators = new Stack<>();
            operands = new Stack<>();
            filters = new Stack<>();
//            criteria = new Stack<>();
        }
    }
}
