package com.microsoft.azure.kusto.data.http;

import com.azure.core.http.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class TestHttpResponse extends HttpResponse {

    private int fakeStatusCode;
    private final HttpHeaders fakeHeaders = new HttpHeaders();

    private TestHttpResponse() {
        super(null);
    }

    protected TestHttpResponse(HttpRequest request) {
        super(request);
    }

    @Override
    public int getStatusCode() {
        return fakeStatusCode;
    }

    private void setStatusCode(int statusCode) {
        this.fakeStatusCode = statusCode;
    }

    @Override
    public String getHeaderValue(String s) {
        return fakeHeaders.getValue(HttpHeaderName.fromString(s));
    }

    @Override
    public HttpHeaders getHeaders() {
        return fakeHeaders;
    }

    private void addHeader(String name, String value) {
        fakeHeaders.add(HttpHeaderName.fromString(name), value);
    }

    @Override
    public Flux<ByteBuffer> getBody() {
        return null;
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
        return null;
    }

    @Override
    public Mono<String> getBodyAsString() {
        return null;
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
        return null;
    }

    public static TestHttpResponseBuilder newBuilder() {
        return new TestHttpResponseBuilder();
    }

    public static class TestHttpResponseBuilder {

        private final TestHttpResponse res = new TestHttpResponse();

        private TestHttpResponseBuilder() {
        }

        public TestHttpResponseBuilder withStatusCode(int statusCode) {
            res.setStatusCode(statusCode);
            return this;
        }

        public TestHttpResponseBuilder addHeader(String name, String value) {
            res.addHeader(name, value);
            return this;
        }

        public TestHttpResponse build() {
            return res;
        }

    }

}