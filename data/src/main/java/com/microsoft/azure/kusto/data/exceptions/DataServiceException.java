// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

public class DataServiceException extends KustoClientException {
    private String ingestionSource;
    private Boolean isPermanent;
    public String getIngestionSource() { return ingestionSource; }

    public DataServiceException(String message) {
        super(message);
    }

    public DataServiceException(String message, Exception exception, Boolean isPermanent) {
        this(null, message, exception, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, Boolean isPermanent) {
        this(ingestionSource, message, null, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, Exception exception, Boolean isPermanent) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
        this.isPermanent = isPermanent;
    }

    /*
     Can return null if permanency is not known
    */
    public Boolean isPermanent(){
        return this.isPermanent;
    }
}
