package com.microsoft.azure.kusto.ingest.exceptions;

public class KustoClientException extends Exception {
    private String m_ingestionSource;

    public String getIngestionSource() { return m_ingestionSource; }

    public KustoClientException(String message) {
        super(message);
    }

    public KustoClientException(String message, Exception exception) {
        super(message, exception);
    }

    public KustoClientException(String ingestionSource, String message, Exception exception) {
        super(message, exception);
        m_ingestionSource = ingestionSource;
    }
}
