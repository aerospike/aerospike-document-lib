package com.aerospike.documentapi.pathparts;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;

/**
 * MapPart is a representation of key access
 */
public class MapPathPart extends PathPart {

    String key;

    public MapPathPart(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public boolean equals(MapPathPart m) {
        return m.key.equals(key);
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
    public Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts) {
        return MapOperation.put(new MapPolicy(), binName, Value.get(key), Value.get(object), contexts);
    }

    @Override
    public Operation toAerospikeDeleteOperation(String binName, CTX[] contexts) {
        return MapOperation.removeByKey(binName, Value.get(key), MapReturnType.NONE, contexts);
    }

    @Override
    public PathPartTypeEnum getType() {
        return PathPartTypeEnum.MAP;
    }
}
