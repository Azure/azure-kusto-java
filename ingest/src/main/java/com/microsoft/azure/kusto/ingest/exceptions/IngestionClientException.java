package com.microsoft.azure.kusto.ingest.exceptions;

public class IngestionClientException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() { return ingestionSource; }

    public IngestionClientException(String message) {
        super(message);
    }

    public IngestionClientException(String message, Exception exception) {
        super(message, exception);
    }

    public IngestionClientException(String ingestionSource, String message, Exception exception) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
