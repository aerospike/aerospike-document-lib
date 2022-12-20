package com.aerospike.documentapi;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

/**
 * Exceptions wrapper. In case of AerospikeException displays its result code name.
 */
public class DocumentApiException extends RuntimeException {

    public DocumentApiException(String message) {
        super(message);
    }

    public DocumentApiException(Exception e) {
        super(e);
    }

    public DocumentApiException(String resultCodeName, AerospikeException ae) {
        super(resultCodeName, ae);
    }

    public static DocumentApiException wrapAerospikeException(AerospikeException ae) {
        return new DocumentApiException(ResultCode.getResultString(ae.getResultCode()), ae);
    }

    /**
     * Utility method to wrap Exception. AerospikeException is processed differently to display its result code name.
     *
     * @param e Exception.
     * @return DocumentApiException.
     */
    public static DocumentApiException toDocumentException(Exception e) {
        return e instanceof AerospikeException ?
                wrapAerospikeException((AerospikeException) e)
                : new DocumentApiException(e);
    }

    /*
     * Exception for displaying different types of json path errors.
     */
    public static class JsonParseException extends DocumentApiException {
        JsonParseException(String message) {
            super(message);
        }
    }

    /**
     * Exception to be thrown in case of invalid json prefix.
     */
    public static class JsonPrefixException extends JsonParseException {
        public JsonPrefixException(String jsonString) {
            super(String.format("'%s' should start with either a '$.' or '$['", jsonString));
        }
    }

    /**
     * Exception to be thrown in case of invalid json path.
     */
    public static class JsonPathException extends JsonParseException {
        public JsonPathException(String jsonString) {
            super(String.format("'%s' does not match key[number] format", jsonString));
        }
    }

    /**
     * Exception to be thrown in case of invalid appending to json.
     */
    public static class JsonAppendException extends JsonParseException {
        public JsonAppendException(String jsonString) {
            super(String.format("Cannot append to '%s'", jsonString));
        }
    }
}
