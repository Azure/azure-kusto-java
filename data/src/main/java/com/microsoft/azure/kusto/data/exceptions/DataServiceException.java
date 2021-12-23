// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.apache.http.HttpResponse;

public class DataServiceException extends KustoDataExceptionBase {
    public DataServiceException(String ingestionSource, String message, boolean isPermanent) {
        this(ingestionSource, message, null, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, Exception exception, boolean isPermanent) {
        super(ingestionSource, message, exception, isPermanent);
    }

    public boolean is404Error() {
        Throwable cause = getCause();
        if (!(cause instanceof DataWebException)) {
            return false;
        }

        HttpResponse httpResponse = ((DataWebException) cause).getHttpResponse();
        if (httpResponse == null) {
            return false;
        }

        return httpResponse.getStatusLine().getStatusCode() == 404;
    }
}
