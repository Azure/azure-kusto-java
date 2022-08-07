package com.microsoft.azure.kusto.data.exceptions;

/**
 * Raised when Kusto client is initialized with an invalid endpoint
 */
public class KustoClientInvalidConnectionStringException extends Exception {
    public KustoClientInvalidConnectionStringException(Exception e) {
        super(e);
    }

    public KustoClientInvalidConnectionStringException(String msg) {
        super(msg);
    }
}