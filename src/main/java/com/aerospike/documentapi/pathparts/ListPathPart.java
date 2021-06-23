package com.aerospike.documentapi.pathparts;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;

/**
 * A ListPart is a representation of a list access
 */
public class ListPathPart extends PathPart {
    int listPosition;

    public ListPathPart(int listPosition) {
        this.listPosition = listPosition;
    }

    public int getListPosition() {
        return listPosition;
    }

    public boolean equals(ListPathPart l) {
        return l.listPosition == listPosition;
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
    public Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts) {
        return ListOperation.insert(binName, listPosition, Value.get(object), contexts);
    }

    @Override
    public Operation toAerospikeDeleteOperation(String binName, CTX[] contexts) {
        return ListOperation.removeByIndex(binName, listPosition, ListReturnType.NONE, contexts);
    }

    @Override
    public PathPartTypeEnum getType() {
        return PathPartTypeEnum.LIST;
    }
}
