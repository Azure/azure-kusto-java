// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.exceptions;

public class IngestionServiceException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() { return ingestionSource; }

    public IngestionServiceException(String message) {
        super(message);
    }

    public IngestionServiceException(String message, Exception exception) {
        super(message, exception);
    }

    public IngestionServiceException(String ingestionSource, String message, Exception exception) {
        this(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
