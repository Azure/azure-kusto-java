// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

/*
  This class represents an error that happened on the client side and is therefore considered permanent
 */
public class DataClientException extends KustoDataException {
    public DataClientException(String ingestionSource, String message) {
        this(ingestionSource, message, null);
    }

    public DataClientException(String ingestionSource, String message, Exception exception) {
        super(ingestionSource, message, exception, true);
    }
}
