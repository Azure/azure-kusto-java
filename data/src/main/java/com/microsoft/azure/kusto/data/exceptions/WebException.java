package com.microsoft.azure.kusto.data.exceptions;

import com.azure.core.http.HttpResponse;
import org.jetbrains.annotations.Nullable;

public class WebException extends Exception {
    @Nullable
    protected final HttpResponse httpResponse;

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
        return httpResponse != null ? httpResponse.getStatusCode() : null;
    }
}
