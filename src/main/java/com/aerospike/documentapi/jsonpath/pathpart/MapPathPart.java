package com.aerospike.documentapi.jsonpath.pathpart;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.documentapi.util.Utils;

import java.util.Objects;

/**
 * MapPart is a representation of key access
 */
public class MapPathPart extends PathPart {

    private final String key;

    public MapPathPart(String key) {
        this.key = key;
    }

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
        if (!(obj instanceof MapPathPart)) {
            return false;
        }
        MapPathPart that = (MapPathPart) obj;
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
    public Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts) throws IllegalArgumentException {
        Utils.validateNotArray(object);

        return MapOperation.put(new MapPolicy(), binName, Value.get(key), Value.get(object), contexts);
    }

    @Override
    public Operation toAerospikeDeleteOperation(String binName, CTX[] contexts) {
        return MapOperation.removeByKey(binName, Value.get(key), MapReturnType.NONE, contexts);
    }

    @Override
    public PathPartType getType() {
        return PathPartType.MAP;
    }
}