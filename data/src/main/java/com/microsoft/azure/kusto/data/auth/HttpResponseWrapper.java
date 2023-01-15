package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpHeader;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.microsoft.aad.msal4j.IHttpResponse;

import org.apache.http.Header;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class wraps our azure HttpResponse, into both the asynchronous HttpResponse class and the synchronous IHttpResponse interface from the azure core
 * library.
 * This class completes the {@link HttpClientWrapper} class, which is responsible for wrapping the http client, this client wraps the response from the
 * client.
 */
public class HttpResponseWrapper extends HttpResponse implements IHttpResponse {
    org.apache.http.HttpResponse response;
    private byte[] body = null;

    protected HttpResponseWrapper(HttpRequest request, org.apache.http.HttpResponse response) {
        super(request);
        this.response = response;
    }

    protected HttpResponseWrapper(org.apache.http.HttpResponse response) {
        this(null, response);
    }

    // HttpResponse methods

    @Override
    public int getStatusCode() {
        return response.getStatusLine().getStatusCode();
    }

    @Override
    public String getHeaderValue(String s) {
        Header firstHeader = response.getFirstHeader(s);
        return firstHeader != null ? firstHeader.getValue() : null;
    }

    @Override
    public HttpHeaders getHeaders() {
        Map<String, List<String>> newHeaders = new HashMap<>();
        Header[] allHeaders = response.getAllHeaders();

        // This is required since one header can have multiple values.
        for (Header header : allHeaders) {
            if (newHeaders.containsKey(header.getName())) {
                newHeaders.get(header.getName()).add(header.getValue());
            } else {
                List<String> values = new ArrayList<>();
                values.add(header.getValue());
                newHeaders.put(header.getName(), values);
            }
        }
        return new HttpHeaders(newHeaders.entrySet().stream().map(e -> new HttpHeader(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }

    @Override
    public Flux<ByteBuffer> getBody() {
        return getBodyAsByteArray().map(ByteBuffer::wrap).flux();
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
        // We read the body only once, and then wrap it in a Mono.

        if (body == null) {
            try {
                body = EntityUtils.toByteArray(response.getEntity());
            } catch (IOException ignored) {
                body = new byte[0];
            }
        }

        return Mono.just(body);
    }

    @Override
    public Mono<String> getBodyAsString() {
        return getBodyAsByteArray().map(String::new);
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
        return getBodyAsByteArray().map(bytes -> new String(bytes, charset));
    }

    // IHttpResponse methods

    @Override
    public int statusCode() {
        return getStatusCode();
    }

    @Override
    public Map<String, List<String>> headers() {
        return getHeaders().stream().collect(Collectors.toMap(com.azure.core.util.Header::getName, com.azure.core.util.Header::getValuesList));
    }

    @Override
    public String body() {
        String block = getBodyAsString().block();
        return block;
    }
}
