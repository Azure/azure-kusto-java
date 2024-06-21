package com.microsoft.azure.kusto.data.req;

import com.azure.core.http.HttpRequest;

public class KustoRequestContext {
    private KustoRequest sdkRequest;
    private HttpRequest httpRequest;

    public KustoRequestContext(KustoRequest sdkRequest, HttpRequest httpRequest) {
        this.sdkRequest = sdkRequest;
        this.httpRequest = httpRequest;
    }

    public KustoRequest getSdkRequest() {
        return sdkRequest;
    }

    public void setSdkRequest(KustoRequest sdkRequest) {
        this.sdkRequest = sdkRequest;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public void setHttpRequest(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }
}
