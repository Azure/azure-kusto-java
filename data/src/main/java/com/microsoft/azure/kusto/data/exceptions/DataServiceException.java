// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

public class DataServiceException extends KustoClientException {
    private String ingestionSource;
    private TriState isPermanent;
    public String getIngestionSource() { return ingestionSource; }

    public DataServiceException(String message) {
        this(null, message, TriState.DONT_KNOW);
    }

    public DataServiceException(String message, Exception exception, TriState isPermanent) {
        this(null, message, exception, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, TriState isPermanent) {
        this(ingestionSource, message, null, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, Exception exception, TriState isPermanent) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
        this.isPermanent = isPermanent;
    }

    public TriState isPermanent(){
        return this.isPermanent;
    }
}
