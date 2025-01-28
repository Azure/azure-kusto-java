package com.microsoft.azure.kusto.data.res;

import com.azure.core.http.HttpResponse;

/**
 * A helper class to store and manage the state during the execution of the
 * asynchronous HTTP request to handle streaming output.
 */
public class ResponseState {

    private boolean returnInputStream;
    private String errorFromResponse;
    private HttpResponse httpResponse;

    public ResponseState() {
        this.returnInputStream = false;
        this.errorFromResponse = null;
        this.httpResponse = null;
    }

    public boolean isReturnInputStream() {
        return returnInputStream;
    }

    public void setReturnInputStream(boolean returnInputStream) {
        this.returnInputStream = returnInputStream;
    }

    public String getErrorFromResponse() {
        return errorFromResponse;
    }

    public void setErrorFromResponse(String errorFromResponse) {
        this.errorFromResponse = errorFromResponse;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public void setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

}
