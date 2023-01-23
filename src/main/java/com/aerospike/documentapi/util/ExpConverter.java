package com.aerospike.documentapi.util;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.documentapi.DocumentApiException;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.token.ContextAwareToken;
import com.aerospike.documentapi.token.ListToken;
import com.aerospike.documentapi.token.MapToken;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aerospike.documentapi.util.Utils.validateJsonPathSingleStep;

/**
 * Utility class for converting JSON path to Aerospike {@link Exp}.
 *
 * <p>Supported JSON paths: containing only map and/or array elements,
 * without wildcards, recursive descent, filter expressions, functions and scripts.
 *
 * <p>Examples of supported JSON paths: </p>
 * <ul>
 * <li>$.store.book,</li>
 * <li>$[0],</li>
 * <li>$.store.book[0],</li>
 * <li>$.store.book[0][1].title.</li>
 * </ul>
 *
 * <p>Examples of unsupported JSON paths: </p>
 * <ul>
 * <li>$.store.book[*].author, </li>
 * <li>$.store..price, </li>
 * <li>$.store.book[?(@.price < 10)]</li>
 * <li>$..book[(@.length-1)]</li>
 * </ul>
 */
@UtilityClass
public class ExpConverter {

    /**
     * Create equal (==) expression.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp eq(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.eq(
                buildExp(binName, value, jsonPathObject),
                getExpValue(value)
        );
    }

    /**
     * Create not equal (!=) expression.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp ne(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.ne(
                buildExp(binName, value, jsonPathObject),
                getExpValue(value)
        );
    }

    /**
     * Create greater than (>) expression.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp gt(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.gt(
                buildExp(binName, value, jsonPathObject),
                getExpValue(value)
        );
    }

    /**
     * Create greater than or equals (>=) expression.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp ge(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.ge(
                buildExp(binName, value, jsonPathObject),
                getExpValue(value)
        );
    }

    /**
     * Create less than (<) expression.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp lt(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.lt(
                buildExp(binName, value, jsonPathObject),
                getExpValue(value)
        );
    }

    /**
     * Create less than or equals (<=) expression.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp le(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.le(
                buildExp(binName, value, jsonPathObject),
                getExpValue(value)
        );
    }

    /**
     * Create expression that performs a regex match on a value specified by a JSON path.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param regex    regular expression string.
     * @param flags    regular expression bit flags. See {@link com.aerospike.client.query.RegexFlag}.
     * @return generated filter expression.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Exp regex(String binName, String jsonPath, String regex, int flags)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        return Exp.regexCompare(
                regex,
                flags,
                buildExp(binName, regex, jsonPathObject)
        );
    }

    private static Exp buildExp(String binName, Object value, JsonPathObject jsonPathObject) {
        List<ContextAwareToken> partList = new ArrayList<>(jsonPathObject.getTokensNotRequiringSecondStepQuery());
        ContextAwareToken lastPart = partList.remove(partList.size() - 1);
        ContextAwareToken rootPart = partList.isEmpty() ? lastPart : partList.get(0);
        CTX[] ctx = partList.stream()
                .map(ContextAwareToken::toAerospikeContext)
                .toArray(CTX[]::new);

        if (lastPart instanceof ListToken) {
            return ListExp.getByIndex(
                    ListReturnType.VALUE,
                    getValueType(value),
                    Exp.val(((ListToken) lastPart).getListPosition()),
                    binExp(binName, rootPart),
                    ctx
            );
        } else if (lastPart instanceof MapToken) {
            return MapExp.getByKey(
                    MapReturnType.VALUE,
                    getValueType(value),
                    Exp.val(((MapToken) lastPart).getKey()),
                    binExp(binName, rootPart),
                    ctx
            );
        } else {
            throw new IllegalArgumentException(String.format("Unexpected path token type '%s'", lastPart.getType()));
        }
    }

    private static Exp binExp(String binName, ContextAwareToken root) {
        if (root instanceof ListToken) {
            return Exp.bin(binName, Exp.Type.LIST);
        }
        return Exp.bin(binName, Exp.Type.MAP);
    }

    private static Exp.Type getValueType(Object value) {
        if (value instanceof Integer || value instanceof Long || value instanceof Short) {
            return Exp.Type.INT;
        } else if (value instanceof Double || value instanceof Float) {
            return Exp.Type.FLOAT;
        } else if (value instanceof String) {
            return Exp.Type.STRING;
        } else if (value instanceof Boolean) {
            return Exp.Type.BOOL;
        } else if (value instanceof List<?>) {
            return Exp.Type.LIST;
        } else if (value instanceof Map<?, ?>) {
            return Exp.Type.MAP;
        } else {
            throw new IllegalArgumentException("Unsupported value type");
        }
    }

    private static Exp getExpValue(Object value) {
        if (value instanceof Integer) {
            return Exp.val((int) value);
        } else if (value instanceof Long) {
            return Exp.val((long) value);
        } else if (value instanceof Short) {
            return Exp.val((short) value);
        } else if (value instanceof Double) {
            return Exp.val((double) value);
        } else if (value instanceof Float) {
            return Exp.val((float) value);
        } else if (value instanceof String) {
            return Exp.val((String) value);
        } else if (value instanceof Boolean) {
            return Exp.val((boolean) value);
        } else if (value instanceof List<?>) {
            return Exp.val((List<?>) value);
        } else if (value instanceof Map<?, ?>) {
            return Exp.val((Map<?, ?>) value);
        } else {
            throw new IllegalArgumentException("Unsupported value type");
        }
    }

    private static String errMsg(String jsonPath) {
        return String.format("Two-step JSON path '%s' cannot be converted to a filter expression", jsonPath);
    }
}
