package com.aerospike.documentapi.pathparts;

import com.aerospike.client.Operation;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListReturnType;

import java.util.Objects;

/**
 * A ListPart is a representation of a list access
 */
public class ListPathPart extends PathPart {

    private final int listPosition;

    public ListPathPart(int listPosition) {
        this.listPosition = listPosition;
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
        if (!(obj instanceof ListPathPart)) {
            return false;
        }
        ListPathPart that = (ListPathPart) obj;
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
    public Operation toAerospikePutOperation(String binName, Object object, CTX[] contexts) {
        return ListOperation.set(binName, listPosition, Value.get(object), contexts);
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
