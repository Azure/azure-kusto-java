// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

public class DataServiceException extends KustoDataExceptionBase {
    public DataServiceException(String ingestionSource, String message, boolean isPermanent) {
        this(ingestionSource, message, null, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, Exception exception, boolean isPermanent) {
        super(ingestionSource, message, exception, isPermanent);
    }

    public boolean is404Error() {
        return getCause() != null && getCause() instanceof DataWebException && ((DataWebException) getCause()).getHttpResponse().getStatusLine().getStatusCode() == 404;
    }
}
