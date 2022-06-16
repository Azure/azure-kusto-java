package com.microsoft.azure.kusto.data.exceptions;

import org.apache.hc.core5.http.HttpResponse;
import org.jetbrains.annotations.Nullable;

public class WebException extends Exception {
    @Nullable protected final HttpResponse httpResponse;

    public WebException(String message, @Nullable HttpResponse httpResponse, Throwable cause) {
        super(message, cause);
        this.httpResponse = httpResponse;
    }

    public @Nullable HttpResponse getHttpResponse() {
        return httpResponse;
    }

    @Override
    public String toString() {
        return this.getMessage();
    }

    @Nullable
    public Integer getStatusCode() {
        return httpResponse != null ? httpResponse.getCode() : null;
    }
}
