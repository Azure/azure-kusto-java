// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.exceptions;

import org.apache.http.HttpResponse;
import org.json.JSONObject;

public class DataWebException extends Exception{

    private String message;
    private HttpResponse httpResponse;
    private OneApiError apiError;

    public String getMessage() { return message; }

    public HttpResponse getHttpResponse() { return httpResponse; }

    public DataWebException(String message, HttpResponse httpResponse) {
        this.message = message;
        this.httpResponse = httpResponse;
        this.apiError = null;
    }

    public OneApiError getApiError() {
        if (apiError == null) {
            JSONObject jsonObject = new JSONObject(getMessage());
            apiError = createApiError(jsonObject);
        }

        return apiError;
    }

    static public OneApiError createApiError(JSONObject obj){
        JSONObject jsonObject = obj.getJSONObject("error");
        return new OneApiError(
                jsonObject.getString("code"),
                jsonObject.getString("message"),
                jsonObject.getString("@message"),
                jsonObject.getString("@type"),
                jsonObject.getJSONObject("@context"),
                jsonObject.getBoolean("@permanent")
        );
    }
}
