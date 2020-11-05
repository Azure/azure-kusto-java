// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

public class DataClientException extends Exception {
    private String ingestionScope;

    public String getIngestionScope() {
        return ingestionScope;
    }

    public DataClientException(String message) {
        super(message);
    }

    public DataClientException(String message, Exception exception) {
        super(message, exception);
    }

    public DataClientException(String ingestionScope, String message, Exception exception) {
        super(message, exception);
        this.ingestionScope = ingestionScope;
    }
}
