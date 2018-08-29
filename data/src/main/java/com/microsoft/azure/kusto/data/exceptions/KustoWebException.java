package com.microsoft.azure.kusto.data.exceptions;

import org.apache.http.HttpResponse;

public class KustoWebException extends Exception{

    private String message;
    private HttpResponse httpResponse;

    public String getMessage() { return message; }

    public HttpResponse getHttpResponse() { return httpResponse; }

    public KustoWebException(String message, HttpResponse httpResponse) {
        this.message = message;
        this.httpResponse = httpResponse;
    }
}
