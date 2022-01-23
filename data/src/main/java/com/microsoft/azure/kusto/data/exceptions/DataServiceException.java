// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.jetbrains.annotations.Nullable;

public class DataServiceException extends KustoDataExceptionBase {
    public DataServiceException(String ingestionSource, String message, boolean isPermanent) {
        this(ingestionSource, message, null, isPermanent);
    }

    public DataServiceException(String ingestionSource, String message, Exception exception, boolean isPermanent) {
        super(ingestionSource, message, exception, isPermanent);
    }

    public boolean is404Error() {
        Integer code = getStatusCode();
        return code != null && code == 404;
    }

    @Nullable
    public Integer getStatusCode() {
        Throwable cause = getCause();
        if (!(cause instanceof WebException)) {
            return null;
        }

        return ((WebException) cause).getStatusCode();
    }
}
