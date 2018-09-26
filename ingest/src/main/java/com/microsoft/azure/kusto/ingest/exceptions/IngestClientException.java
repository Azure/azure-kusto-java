package com.microsoft.azure.kusto.ingest.exceptions;

public class IngestClientException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() { return ingestionSource; }

    public IngestClientException(String message) {
        super(message);
    }

    public IngestClientException(String message, Exception exception) {
        super(message, exception);
    }

    public IngestClientException(String ingestionSource, String message, Exception exception) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
