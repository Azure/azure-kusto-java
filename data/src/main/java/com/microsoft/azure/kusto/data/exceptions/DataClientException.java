package com.microsoft.azure.kusto.data.exceptions;

public class DataClientException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() { return ingestionSource; }

    public DataClientException(String message) {
        super(message);
    }

    public DataClientException(String message, Exception exception) {
        super(message, exception);
    }

    public DataClientException(String ingestionSource, String message, Exception exception) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
