package com.microsoft.azure.kusto.data.req;

import com.azure.core.http.HttpRequest;

public class KustoRequest {
    private KustoQuery kq;
    private HttpRequest httpRequest;

    public KustoRequest(KustoQuery kq, HttpRequest httpRequest) {
        this.kq = kq;
        this.httpRequest = httpRequest;
    }

    public KustoQuery getKq() {
        return kq;
    }

    public void setKq(KustoQuery kq) {
        this.kq = kq;
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public void setHttpRequest(HttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }
}
