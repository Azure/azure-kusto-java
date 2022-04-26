package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHeaders;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import reactor.core.publisher.Mono;

public class HttpClientWrapper implements com.azure.core.http.HttpClient, IHttpClient {
    private final HttpClient httpClient;

    HttpClientWrapper(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest httpRequest) {
        HttpUriRequest request;
        String uri = httpRequest.getUrl().toString();
        switch (httpRequest.getHttpMethod()) {
            case GET:
                request = new HttpGet(uri);
                break;
            case POST:
                request = new HttpPost(uri);
                break;
            case PUT:
                request = new HttpPut(uri);
                break;
            case PATCH:
                request = new HttpPatch(uri);
                break;
            case DELETE:
                request = new HttpDelete(uri);
                break;
            case HEAD:
                request = new HttpHead(uri);
                break;
            case OPTIONS:
                request = new HttpOptions(uri);
                break;
            case TRACE:
                request = new HttpTrace(uri);
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + httpRequest.getHttpMethod());
        }
        request.setHeaders(httpRequest.getHeaders().stream().map(h -> new BasicHeader(h.getName(), h.getValue())).toArray(Header[]::new));
        Mono before = Mono.empty();
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest entityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            PipedOutputStream osPipe = new PipedOutputStream();
            PipedInputStream isPipe = null;
            try {
                isPipe = new PipedInputStream(osPipe);
            } catch (IOException e) {
            }

            before = Mono.from(httpRequest.getBody().map(buf -> {
                try {
                    osPipe.write(buf.array());
                } catch (IOException e) {
                }
                return buf;
            }));
            entityEnclosingRequest.setEntity(new InputStreamEntity(isPipe));
        }

        return before.flatMap(a -> Mono.create(monoSink -> {
            try {
                org.apache.http.HttpResponse response = httpClient.execute(request);
                monoSink.success(new HttpResponseWrapper(httpRequest, response));
            } catch (IOException e) {
                monoSink.error(e);
            }
        }));
    }

    @Override
    public IHttpResponse send(com.microsoft.aad.msal4j.HttpRequest httpRequest) throws Exception {
        HttpUriRequest request;
        String uri = httpRequest.url().toString();
        switch (httpRequest.httpMethod()) {
            case GET:
                request = new HttpGet(uri);
                break;
            case POST:
                request = new HttpPost(uri);
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + httpRequest.httpMethod());
        }
        request.setHeaders(httpRequest.headers().entrySet().stream().map(h -> new BasicHeader(h.getKey(), h.getValue())).toArray(Header[]::new));
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest entityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            String body = httpRequest.body();
            entityEnclosingRequest.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        }
        return new HttpResponseWrapper(httpClient.execute(request));
    }
}
