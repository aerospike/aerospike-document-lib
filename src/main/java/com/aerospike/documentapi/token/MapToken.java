package com.aerospike.documentapi.token;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.documentapi.util.Utils;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOC_ROOT;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.WILDCARD;

/**
 * MapPart is a representation of key access
 */
public class MapToken extends ContextAwareToken {

    private final String key;

    public MapToken(String key) {
        this.key = key;
    }

    static final Pattern PATH_PATTERN = Pattern.compile("^([^\\[^\\]]*)(\\[(\\d+)\\])*$");

    public String getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MapToken)) {
            return false;
        }
        MapToken that = (MapToken) obj;
        return key.equals(that.key);
    }

    @Override
    public CTX toAerospikeContext() {
        return CTX.mapKey(Value.get(key));
    }

    @Override
    public Operation toAerospikeGetOperation(String binName, CTX[] contexts) {
        return MapOperation.getByKey(binName, Value.get(key), MapReturnType.VALUE, contexts);
    }

    @Override
    public Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts)
            throws IllegalArgumentException {
        Utils.validateNotArray(object);

        return MapOperation.put(new MapPolicy(), binName, Value.get(key), Value.get(object), contexts);
    }

    @Override
    public Operation toAerospikeDeleteOperation(String binName, CTX[] contexts) {
        return MapOperation.removeByKey(binName, Value.get(key), MapReturnType.NONE, contexts);
    }

    @Override
    public TokenType getType() {
        return TokenType.MAP;
    }

    @Override
    public boolean requiresJsonQuery() {
        return false;
    }

    @Override
    public boolean read(String strPart) {
        setString(key);

        return true;
    }

    public static Optional<Token> match(String strPart) {
        Token token;

        Matcher keyMatcher = PATH_PATTERN.matcher(strPart);
        if ((!strPart.contains("[")) && (!strPart.contains("]"))) {
            // ignoring * wildcard after a dot, it's the same as ending with a .path
            if (!strPart.equals(String.valueOf(WILDCARD)) && !strPart.equals(String.valueOf(DOC_ROOT))) {
                token = new MapToken(strPart);
                token.read(strPart);
                return Optional.of(token);
            }
        } else if (keyMatcher.find()) {
            String key = keyMatcher.group(1);
            if (!key.equals(String.valueOf(DOC_ROOT))) {
                token = new MapToken(strPart);
                token.read(strPart);
                return Optional.of(token);
            }
        }

        return Optional.empty();
    }
}
