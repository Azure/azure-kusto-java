// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.exceptions;

import com.azure.core.exception.AzureException;

public class IngestionClientException extends AzureException {
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

    public IngestionClientException(String ingestionSource, String message, RuntimeException exception) {
        this(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
