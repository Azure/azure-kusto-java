package com.microsoft.azure.kusto.data.exceptions;

/**
 * Raised when Kusto client is initialized with an invalid endpoint
 */
public class KustoClientInvalidConnectionStringException extends DataClientException {
    public KustoClientInvalidConnectionStringException(Exception e) {
        super(e);
    }

    public KustoClientInvalidConnectionStringException(String msg) {
        super(msg);
    }

    public KustoClientInvalidConnectionStringException(String clusterURL, String msg, Exception e) {
        super(clusterURL, msg, e);
    }
}
