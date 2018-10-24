package com.microsoft.azure.kusto.data.exceptions;

public class DataServiceException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() { return ingestionSource; }

    public DataServiceException(String message) {
        super(message);
    }

    public DataServiceException(String message, Exception exception) {
        super(message, exception);
    }

    public DataServiceException(String ingestionSource, String message, Exception exception) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
    }

    public DataServiceException(String ingestionSource, String message) {
        super(message);
        this.ingestionSource = ingestionSource;
    }
}
