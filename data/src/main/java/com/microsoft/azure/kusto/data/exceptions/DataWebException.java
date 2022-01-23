// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.apache.http.HttpResponse;
import org.json.JSONObject;

public class DataWebException extends WebException {

    private final OneApiError apiError;

    public DataWebException(String message, HttpResponse httpResponse, Throwable cause) {
        super(message, httpResponse, cause);
        this.apiError = OneApiError.fromJsonObject(new JSONObject(getMessage()).getJSONObject("error"));
    }

    public DataWebException(String message, HttpResponse httpResponse) {
        this(message, httpResponse, null);
    }

    public DataWebException(String message) {
        this(message, null, null);
    }

    public OneApiError getApiError() {
        return apiError;
    }
}
