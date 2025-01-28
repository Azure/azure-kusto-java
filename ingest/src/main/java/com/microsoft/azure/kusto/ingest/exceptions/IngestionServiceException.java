// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.exceptions;

import com.azure.core.exception.AzureException;

public class IngestionServiceException extends AzureException { // TODO: remove throws from internal method declarations (not from public Apis) on async ingest
                                                                // impl
    private String ingestionSource;

    public String getIngestionSource() {
        return ingestionSource;
    }

    public IngestionServiceException(String message) {
        super(message);
    }

    public IngestionServiceException(String message, Exception exception) {
        super(message, exception);
    }

    public IngestionServiceException(String ingestionSource, String message, RuntimeException exception) {
        this(message, exception);
        this.ingestionSource = ingestionSource;
    }
}
