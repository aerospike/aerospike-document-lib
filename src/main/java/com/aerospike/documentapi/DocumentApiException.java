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

    public DocumentApiException(AerospikeException e) {
        super(e);
    }

    /**
     * Utility method to categorise particular types of exceptions we encounter.
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

    /**
     * Thrown if a non-existing map or list is accessed, and also if accessing a list element out of
     * existing list bounds.
     */
    public static class ObjectNotFoundException extends DocumentApiException {
        public ObjectNotFoundException(AerospikeException e) {
            super(e);
        }

        public ObjectNotFoundException() {
            super();
        }
    }

    /**
     * Thrown if accessing a list as if it were a map, or looking for a key in a map that doesn't exist.
     */
    public static class KeyNotFoundException extends DocumentApiException {
        public KeyNotFoundException(AerospikeException e) {
            super(e);
        }
    }

    /**
     * Thrown when there is type mismatch, e.g. accessing a map as if it were a list.
     */
    public static class TypeMismatchException extends DocumentApiException {
        public TypeMismatchException(AerospikeException e) {
            super(e);
        }
    }

    /*
     * Different types of json path exceptions
     */
    public static class JsonParseException extends DocumentApiException {
        JsonParseException(String message) {
            super(message);
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

    public static class JsonAppendException extends JsonParseException {
        public JsonAppendException(String jsonString) {
            super(String.format("Cannot append to '%s'", jsonString));
        }
    }
}
