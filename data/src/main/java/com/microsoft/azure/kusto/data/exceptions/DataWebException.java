// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.apache.http.HttpResponse;
import org.json.JSONObject;

public class DataWebException extends Exception{

    private final HttpResponse httpResponse;
    private OneApiError apiError;

    public HttpResponse getHttpResponse() { return httpResponse; }

    public DataWebException(String message, HttpResponse httpResponse) {
        super(message);
        this.httpResponse = httpResponse;
        this.apiError = null;
    }

    public DataWebException(String message) {
        this(message, null);
    }

    public OneApiError getApiError() {
        if (apiError == null) {
            JSONObject jsonObject = new JSONObject(getMessage()).getJSONObject("error");
            apiError = new OneApiError(
                    jsonObject.getString("code"),
                    jsonObject.getString("message"),
                    jsonObject.getString("@message"),
                    jsonObject.getString("@type"),
                    jsonObject.getJSONObject("@context"),
                    jsonObject.getBoolean("@permanent")
            );
        }

        return apiError;
    }

    @Override
    public String toString() {
        return this.getMessage();
    }
}
