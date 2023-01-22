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
import java.util.Map;

import static com.aerospike.documentapi.util.Utils.validateJsonPathSingleStep;

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
     * Create not equal (!=) Filter.
     *
     * @param binName  document bin name in a record.
     * @param jsonPath JSON path to build a filter expression from.
     * @param value    value object to compare with.
     * @return generated Filter.
     * @throws DocumentApiException if fails to parse the jsonPath.
     */
    public static Filter ne(String binName, String jsonPath, Object value) // TODO: can this be implemented?
            throws DocumentApiException {
//        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
//        Number val = getNumber(value);
//        if (val == null) {
//            return Filter.equal(binName, value.toString(), getCTX(jsonPathObject));
//        } else {
//            return Filter.equal(binName, val.longValue(), getCTX(jsonPathObject));
//        }
        return null;
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
    public static Filter lt(String binName, String jsonPath, Object value, IndexCollectionType indexCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'<' operator can be applied only to a number"); // TODO: DocumentApiException? null?
        } else {
            return Filter.range(binName, Long.MIN_VALUE, val.longValue() - 1, getCTX(jsonPathObject));
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
    public static Filter gt(String binName, String jsonPath, Object value, IndexCollectionType indexCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'>' operator can be applied only to a number");
        } else {
            return Filter.range(binName, val.longValue() + 1, Long.MAX_VALUE, getCTX(jsonPathObject));
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
    public static Filter lte(String binName, String jsonPath, Object value, IndexCollectionType indexCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'<=' operator can be applied only to a number");
        } else {
            return Filter.range(binName, Long.MIN_VALUE, val.longValue(), getCTX(jsonPathObject));
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
    public static Filter gte(String binName, String jsonPath, Object value, IndexCollectionType indexCollectionType)
            throws DocumentApiException {
        JsonPathObject jsonPathObject = validateJsonPathSingleStep(new JsonPathParser().parse(jsonPath), errMsg(jsonPath));
        Number val = getNumber(value);
        if (val == null) {
            throw new IllegalArgumentException("'>=' operator can be applied only to a number");
        } else {
//            return Filter.range(binName, val.longValue(), Long.MAX_VALUE, getCTX(jsonPathObject));
//            return Filter.range(binName, val.longValue(), Long.MAX_VALUE);
            return Filter.range(binName, indexCollectionType, val.longValue(), Long.MAX_VALUE, getCTX(jsonPathObject));
//            return Filter.range(binName, indexCollectionType, val.longValue(), Long.MAX_VALUE);
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
        } else if (value instanceof Double) { // TODO: as String or unsupported?
            return null;
        } else if (value instanceof Float) { // TODO: ?
            return null;
        } else if (value instanceof String) {
            return null;
        } else if (value instanceof Boolean) {
            return null;
        } else if (value instanceof List<?>) { // TODO: ?
            return null;
        } else if (value instanceof Map<?, ?>) { // TODO: ?
            return null;
        } else {
            throw new IllegalArgumentException("Unsupported value type");
        }
    }

    private static CTX[] getCTX(JsonPathObject jsonPathObject) {
        List<ContextAwareToken> partList = new ArrayList<>(jsonPathObject.getTokensNotRequiringSecondStepQuery());
//        ContextAwareToken lastPart = partList.remove(partList.size() - 1);
        return partList.stream()
                .map(ContextAwareToken::toAerospikeContext)
                .toArray(CTX[]::new);
    }
}
