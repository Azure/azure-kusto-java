package com.microsoft.azure.kusto.data.exceptions;

import com.azure.core.exception.AzureException;

public abstract class KustoDataExceptionBase extends AzureException {
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
