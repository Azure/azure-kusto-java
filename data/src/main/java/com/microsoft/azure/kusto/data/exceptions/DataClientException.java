// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

/*
  This class represents an error that happened on the client side and is therefore considered permanent
 */
public class DataClientException extends Exception {
    private String ingestionSource;

    public String getIngestionSource() {
        return ingestionSource;
    }

    public DataClientException(String message) {
        super(message);
    }

    public DataClientException(String message, Exception exception) {
        super(message, exception);
    }

    public DataClientException(String ingestionSource, String message, Exception exception) {
        super(message, exception);
        this.ingestionSource = ingestionSource;
    }

    public Boolean isPermanent(){
        return true;
    }
}
