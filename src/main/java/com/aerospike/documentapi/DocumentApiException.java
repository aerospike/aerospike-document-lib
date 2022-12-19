package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

/**
 * Classes used to type the errors that can be returned.
 */
public class DocumentApiException extends Exception {

    public DocumentApiException(String message) {
        super(message);
    }

    public DocumentApiException() {
        super();
    }

    /**
     * Thrown if a non-existing map or list is accessed, and also if accessing a list element out of
     * existing list bounds.
     */
    public static class ObjectNotFoundException extends DocumentApiException {
        public ObjectNotFoundException(AerospikeException e) {
            super();
        }

        public ObjectNotFoundException() {
            super();
        }
    }

    /**
     * Thrown if accessing a list as if it was a map, or looking for a key in a map that doesn't exist.
     */
    public static class KeyNotFoundException extends DocumentApiException {
        public KeyNotFoundException(AerospikeException e) {
            super();
        }
    }

    /**
     * Thrown when there is type mismatch, e.g. accessing a map as if it was a list.
     */
    public static class TypeMismatchException extends DocumentApiException {
        public TypeMismatchException(AerospikeException e) {
            super();
        }
    }

    /*
     * Different types of json path exceptions
     */
    public static class JsonParseException extends DocumentApiException {
        JsonParseException(String str) {
            super(str);
        }
    }

    public static class JsonPrefixException extends JsonParseException {
        public JsonPrefixException(String jsonString) {
            super(String.format("'%s' should start with either a '$.' or '$['", jsonString));
        }
    }

    public static class JsonPathException extends JsonParseException {
        public JsonPathException(String jsonString) {
            super(String.format("'%s' does not match key[number] format", jsonString));
        }
    }

    public static class ListException extends JsonParseException {
        public ListException(String jsonString) {
            super(String.format("You cannot append to document root '%s'", jsonString));
        }
    }

    /**
     * Utility method to categorise the different sort of exceptions we encounter.
     *
     * @param e AerospikeException.
     * @return case-specific exception or throw the original AerospikeException.
     */
    public static DocumentApiException toDocumentException(AerospikeException e) {
        if (e.getResultCode() == ResultCode.PARAMETER_ERROR) {
            return new KeyNotFoundException(e);
        } else if (e.getResultCode() == ResultCode.BIN_TYPE_ERROR) {
            return new TypeMismatchException(e);
        } else if (e.getResultCode() == ResultCode.OP_NOT_APPLICABLE) {
            return new ObjectNotFoundException(e);
        } else {
            throw e;
        }
    }
}
