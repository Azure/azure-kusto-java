package com.microsoft.azure.kusto.data.exceptions;

public class KustoClientInvalidConnectionStringException extends Exception {
    public KustoClientInvalidConnectionStringException(Exception e) {
        super(e);
    }

    public KustoClientInvalidConnectionStringException(String msg) {
        super(msg);
    }
}