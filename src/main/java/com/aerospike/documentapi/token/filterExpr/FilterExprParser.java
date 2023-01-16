package com.aerospike.documentapi.token.filterExpr;

import com.aerospike.client.exp.Expression;
import com.aerospike.documentapi.token.filterExpr.FilterExprFactory.FilterCriterion;
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
    public static FilterCriterion parse(String expression) {
        List<String> tokens = parseExpressionString(expression);
        Stack<NestedBlock> stack = new Stack<>();
        stack.push(new NestedBlock());

        for (String token : tokens) {
            token = token.trim();
            if (token.equals("(")) {
                stack.push(new NestedBlock());
            } else if (token.equals(")")) {
                NestedBlock block = stack.pop();
                checkUnaryLogicalOpExp(block);
                FilterCriterion criterion = block.criteria.get(0);
                if (criterion == null) {
                    throw new RuntimeException("Empty block criteria stack");
                }
                if (!block.logicOperators.isEmpty()) {
                    Preconditions.checkState(forAllEqual(block.logicOperators),
                            String.format("Only equal logic operations are required on the same level, cannot process '%s'." +
                                            " Use parentheses to combine both logical operators if needed.",
                                    asString(block.logicOperators)));
                    criterion = FilterExprFactory.getLogicalCriterion(block.logicOperators.pop(),
                            block.criteria.toArray(new FilterCriterion[0]));
                }
                stack.peek().criteria.push(criterion);
            } else if (Operator.isSimple(token)) {
                stack.peek().relationalOperators.push(token);
            } else if (Operator.isLogic(token)) {
                stack.peek().logicOperators.push(token);
            } else if (Operator.isLogicUnary(token)) {
                stack.peek().unaryLogicOperators.push(token);
            } else if (Operator.isSpecial(token)) {
                stack.peek().relationalOperators.push(token); // more will be added with tests for complex expr
            } else {
                // is operand
                if (stack.peek().relationalOperators.size() > 0) {
                    if (stack.peek().operands.size() == 1) {
                        FilterCriterion criterion = FilterExprFactory.getCompareCriterion(
                                stack.peek().relationalOperators.pop(),
                                stack.peek().operands.pop(),
                                token
                        );
                        stack.peek().criteria.push(criterion);
                    }
                } else {
                    stack.peek().operands.push(token);
                }
            }
        }

        NestedBlock block = stack.pop();
        FilterCriterion criterion = block.criteria.get(0);
        if (!block.logicOperators.isEmpty()) {
            Preconditions.checkState(forAllEqual(block.logicOperators),
                    String.format("Only equal logic operations are required on the same level, cannot process '%s'." +
                                    " Use parentheses to combine both logical operators if needed.",
                            asString(block.logicOperators)));
            criterion = FilterExprFactory.getLogicalCriterion(block.logicOperators.peek(),
                    block.criteria.toArray(new FilterCriterion[0]));
        }

        return criterion;
    }

    private static void checkUnaryLogicalOpExp(NestedBlock block) {
        if (block.operands.size() == 1
                && block.relationalOperators.isEmpty()
                && block.unaryLogicOperators.isEmpty()
        ) {
            if (block.operands.peek().startsWith(NOT_EXISTS.toString())) {
                block.unaryLogicOperators.push(NOT_EXISTS.toString());
            } else {
                block.unaryLogicOperators.push(EXISTS.toString());
            }
            FilterCriterion criterion = FilterExprFactory.getUnaryLogicalCriterion(
                    block.unaryLogicOperators.pop(),
                    block.operands.pop()
            );
            block.criteria.push(criterion);
        }
    }

    private static String asString(Stack<String> stack) {
        return String.join(",", stack);
    }

    private static class NestedBlock {
        Stack<String> relationalOperators;
        Stack<String> logicOperators;
        Stack<String> unaryLogicOperators;
        Stack<String> operands;
        Stack<FilterCriterion> criteria;

        NestedBlock() {
            relationalOperators = new Stack<>();
            logicOperators = new Stack<>();
            unaryLogicOperators = new Stack<>();
            operands = new Stack<>();
            criteria = new Stack<>();
        }
    }
}
