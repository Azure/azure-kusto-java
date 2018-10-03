package com.microsoft.azure.kusto.data.exceptions;

import org.apache.http.HttpResponse;

public class WebException extends Exception{

    private String message;
    private HttpResponse httpResponse;

    public String getMessage() { return message; }

    public HttpResponse getHttpResponse() { return httpResponse; }

    public WebException(String message, HttpResponse httpResponse) {
        this.message = message;
        this.httpResponse = httpResponse;
    }
}
