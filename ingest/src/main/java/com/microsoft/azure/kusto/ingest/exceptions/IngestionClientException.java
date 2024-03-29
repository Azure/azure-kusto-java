// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.exceptions;

public class IngestionClientException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() {
        return ingestionSource;
    }

    public IngestionClientException(String message) {
        super(message);
    }

    public IngestionClientException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public IngestionClientException(String ingestionSource, String message, Exception exception) {
        this(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
