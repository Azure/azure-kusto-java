// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

/*
  This class represents an error that happened on the client side and is therefore considered permanent
 */
public class DataClientException extends KustoDataExceptionBase {
    public DataClientException(Exception ex) {
        this(ex.getMessage());
    }

    public DataClientException(String message) {
        this(null, message);
    }

    public DataClientException(String ingestionSource, String message) {
        this(ingestionSource, message, null);
    }

    public DataClientException(String ingestionSource, String message, Exception exception) {
        super(ingestionSource, message, exception, true);
    }

    public static DataClientException unwrapThrowable(String clusterUrl, Throwable throwable) {
        if (throwable instanceof DataClientException) {
            return (DataClientException) throwable;
        }
        if (throwable instanceof Exception) {
            Exception ex = (Exception) throwable;
            return new DataClientException(clusterUrl, ExceptionsUtils.getMessageEx(ex), ex);
        }

        return new DataClientException(clusterUrl, throwable.toString(), null);
    }
}
