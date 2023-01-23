package com.aerospike.documentapi.util;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.documentapi.DocumentApiException;
import com.aerospike.documentapi.jsonpath.JsonPathObject;
import com.aerospike.documentapi.jsonpath.JsonPathParser;
import com.aerospike.documentapi.token.ContextAwareToken;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.documentapi.util.Utils.validateJsonPathSingleStep;

/**
 * Utility class for converting JSON path to Aerospike {@link Filter}.
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
public class FilterConverter {

    /**
     * Create equal (==) Filter.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated Filter.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Filter eq(String binName, String jsonPath, Object value)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            return Filter.equal(binName, value.toString(), getCTX(jsonPathObject));
        } else {
            return Filter.equal(binName, val.longValue(), getCTX(jsonPathObject));
        }
    }

    /**
     * Create less than (<) Filter.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated Filter.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Filter lt(String binName, String jsonPath, Object value, IndexCollectionType idxCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'<' operator can be applied only to a number");
        } else {
            return Filter.range(binName, idxCollectionType, Long.MIN_VALUE, val.longValue() - 1,
                    getCTX(jsonPathObject));
        }
    }

    /**
     * Create greater than (>) Filter.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated Filter.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Filter gt(String binName, String jsonPath, Object value, IndexCollectionType idxCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'>' operator can be applied only to a number");
        } else {
            return Filter.range(binName, idxCollectionType, val.longValue() + 1, Long.MAX_VALUE,
                    getCTX(jsonPathObject));
        }
    }

    /**
     * Create less than or equals (<=) Filter.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated Filter.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Filter le(String binName, String jsonPath, Object value, IndexCollectionType idxCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'<=' operator can be applied only to a number");
        } else {
            return Filter.range(binName, idxCollectionType, Long.MIN_VALUE, val.longValue(), getCTX(jsonPathObject));
        }
    }

    /**
     * Create greater than or equals (>) Filter.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated Filter.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Filter ge(String binName, String jsonPath, Object value, IndexCollectionType idxCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'>=' operator can be applied only to a number");
        } else {
            return Filter.range(binName, idxCollectionType, val.longValue(), Long.MAX_VALUE, getCTX(jsonPathObject));
        }
    }

    private static String errMsg(String jsonPath) {
        return String.format("Two-step JSON path '%s' cannot be converted to a Filter", jsonPath);
    }

    private static Number getNumber(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        } else if (value instanceof Long) {
            return (long) value;
        } else if (value instanceof Short) {
            return (long) value;
        } else if (value instanceof String) {
            return null;
        } else if (value instanceof Boolean) {
            return null;
        } else {
            throw new IllegalArgumentException("Unsupported value type");
        }
    }

    private static CTX[] getCTX(JsonPathObject jsonPathObject) {
        List<ContextAwareToken> partList = new ArrayList<>(jsonPathObject.getTokensNotRequiringSecondStepQuery());
        return partList.stream()
                .map(ContextAwareToken::toAerospikeContext)
                .toArray(CTX[]::new);
    }
}
