package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

/**
 * Classes used to type the errors that can be returned.
 */
public class DocumentApiException extends Exception {

    AerospikeException e;

    public DocumentApiException(AerospikeException e) {
        this.e = e;
    }

    /**
     * Thrown if a map or list is accessed that doesn't exist. Also if accessing a list element out of
     * existing list bounds.
     */
    public static class ObjectNotFoundException extends DocumentApiException {
        public ObjectNotFoundException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Thrown if accessing a list as if it was a map, or looking for a key in a map that doesn't exist.
     */
    public static class KeyNotFoundException extends DocumentApiException {
        public KeyNotFoundException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Thrown if accessing a map as if it were a list or looking for a list element in a list that doesn't exist.
     */
    public static class NotAListException extends DocumentApiException {
        public NotAListException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Utility method to categorise the different sort of exceptions we will encounter.
     *
     * @param e An AerospikeException.
     * @return A more descriptive case-specific exception.
     */
    public static DocumentApiException toDocumentException(AerospikeException e) {
        if (e.getResultCode() == ResultCode.PARAMETER_ERROR) {
            return new KeyNotFoundException(e);
        } else if (e.getResultCode() == ResultCode.BIN_TYPE_ERROR) {
            return new NotAListException(e);
        } else if (e.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
            return new ObjectNotFoundException(e);
        } else {
            throw e;
        }
    }
}
