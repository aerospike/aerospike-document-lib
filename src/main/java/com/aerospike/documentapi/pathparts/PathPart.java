package com.aerospike.documentapi.pathparts;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;

/**
 * AccessPathPart analysis is ultimately used to create CTX (context) objects and operations
 */
public abstract class PathPart {
    public abstract CTX toAerospikeContext();

    public abstract Operation toAerospikeGetOperation(String binName, CTX[] contexts);

    public abstract Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts) throws IllegalArgumentException;

    public Operation toAerospikeAppendOperation(String binName, Object object, CTX[] contexts) throws IllegalArgumentException {
        validateNotArray(object);

        return ListOperation.append(binName, Value.get(object), contexts);
    }

    public abstract Operation toAerospikeDeleteOperation(String binName, CTX[] contexts);

    public abstract PathPartTypeEnum getType();

    protected void validateNotArray(Object object) throws IllegalArgumentException {
        if (object.getClass().isArray()) {
            throw new IllegalArgumentException("Putting/appending an array is yet not allowed, consider providing a Collection");
        }
    }
}
