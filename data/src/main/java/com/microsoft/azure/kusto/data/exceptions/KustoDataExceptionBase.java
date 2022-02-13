// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

public abstract class KustoDataExceptionBase extends Exception {
    private final String ingestionSource;
    private final boolean isPermanent;

    protected KustoDataExceptionBase(String ingestionSource, String message, Exception exception, boolean isPermanent) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
        this.isPermanent = isPermanent;
    }

    public boolean isPermanent() {
        return isPermanent;
    }

    public String getIngestionSource() {
        return ingestionSource;
    }
}
