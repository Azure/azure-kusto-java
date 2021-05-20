package com.microsoft.azure.kusto.data.exceptions;

public abstract class KustoClientException extends Exception {
    public KustoClientException(String message) {
        super(message);
    }

    public KustoClientException(String message, Exception exception) {
        super(message, exception);
    }

    public abstract TriState isPermanent();
}
