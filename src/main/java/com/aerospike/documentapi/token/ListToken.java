package com.aerospike.documentapi.token;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.documentapi.util.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.aerospike.documentapi.jsonpath.JsonPathParser.CLOSE_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.DOC_ROOT;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.OPEN_BRACKET;
import static com.aerospike.documentapi.jsonpath.JsonPathParser.WILDCARD;

/**
 * A ListPart is a representation of a list access
 */
public class ListToken extends ContextAwareToken {

    static final Pattern PATH_PATTERN = Pattern.compile("^([^\\[^\\]]*)(\\[([\\*\\d]+)\\])*$");
    static final Pattern INDEX_PATTERN = Pattern.compile("(\\[([\\*\\d]+)\\])");

    private final int listPosition;

    public ListToken(int listPosition) {
        this.listPosition = listPosition;
        setString(OPEN_BRACKET + String.valueOf(listPosition) + CLOSE_BRACKET);
    }

    // For reading a path part into a list of tokens (map, list or wildcard).
    // Appends parsed tokens to a list.
    // Expected form of path part is key[index1][index2].
    public static List<Token> parseToList(String strPart) {
        List<Token> list = new ArrayList<>();
        Token token;

        Matcher keyMatcher = PATH_PATTERN.matcher(strPart);
        if ((!strPart.contains("[")) && (!strPart.contains("]"))) {
            // ignoring * wildcard after a dot
            if (!strPart.equals(String.valueOf(WILDCARD)) && !strPart.equals(String.valueOf(DOC_ROOT))) {
                token = new MapToken(strPart);
                list.add(token);
            }
        } else if (keyMatcher.find()) {
            String key = keyMatcher.group(1);
            if (!key.equals(String.valueOf(DOC_ROOT))
                    && key.length() > 0
                    && key.charAt(0) != OPEN_BRACKET && key.charAt(key.length() - 1) != CLOSE_BRACKET) {
                token = new MapToken(key);
                list.add(token);
            }
        }

        Matcher indexMatcher = INDEX_PATTERN.matcher(strPart);
        while (indexMatcher.find()) {
            String res = indexMatcher.group(2);
            if (res.equals("*")) {
                token = new WildcardToken(res, true);
            } else {
                token = new ListToken(Integer.parseInt(res));
            }
            list.add(token);
        }

        return list;
    }

    public int getListPosition() {
        return listPosition;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(listPosition);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ListToken)) {
            return false;
        }
        ListToken that = (ListToken) obj;
        return listPosition == that.listPosition;
    }

    @Override
    public CTX toAerospikeContext() {
        return CTX.listIndex(listPosition);
    }

    @Override
    public Operation toAerospikeGetOperation(String binName, CTX[] contexts) {
        return ListOperation.getByIndex(binName, listPosition, ListReturnType.VALUE, contexts);
    }

    @Override
    public Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts)
            throws IllegalArgumentException {
        Utils.validateNotArray(object);

        return ListOperation.set(binName, listPosition, Value.get(object), contexts);
    }

    @Override
    public Operation toAerospikeDeleteOperation(String binName, CTX[] contexts) {
        return ListOperation.removeByIndex(binName, listPosition, ListReturnType.NONE, contexts);
    }

    @Override
    public TokenType getType() {
        return TokenType.LIST;
    }
}
