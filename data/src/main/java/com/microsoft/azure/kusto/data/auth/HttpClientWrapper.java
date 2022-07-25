package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.microsoft.aad.msal4j.HttpMethod;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpOptions;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;

import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpTrace;
import org.apache.hc.client5.http.classic.methods.HttpUriRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicHeader;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This class wraps both of the azure http client interfaces - IHttpClient and HttpClient to use our apache http client.
 * These interfaces are required by the azure authentication classes - IHttpClient for Managed Identity, and HttpClient for the rest.
 * HttpClient is synchronous, so the implementation is straight-forward.
 * IHttpClient is asynchronous, so we need to be more clever about integrating it with the synchronous apache client.
 */
public class HttpClientWrapper implements com.azure.core.http.HttpClient, IHttpClient {
    private final HttpClient httpClient;

    HttpClientWrapper(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    // Implementation of the asynchronous IHttpClient
    @Override
    public Mono<HttpResponse> send(HttpRequest httpRequest) {
        // Creating the apache request

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
        // Translating the headers

        request.setHeaders(httpRequest.getHeaders().stream().map(h -> new BasicHeader(h.getName(), h.getValue())).toArray(Header[]::new));

        // Setting the request's body/entity

        // This empty operation
        Mono before = Mono.empty();

        // This creates the Mono (similar to a future) that will be used to write the body, we will have
        Flux<ByteBuffer> body = httpRequest.getBody();
        if (body != null) {
            PipedOutputStream osPipe = new PipedOutputStream();
            PipedInputStream isPipe = null;
            try {
                isPipe = new PipedInputStream(osPipe);
            } catch (IOException ignored) {
                // This should never happen
            }

            before = Mono.from(body.map(buf -> {
                try {
                    // Ignore the warning saying it's a blocking operation - since we are controlling both pipes, it shouldn't be
                    osPipe.write(buf.array());
                } catch (IOException e) {
                    // If this happens there's not much we can do here
                }
                return buf;
            }));

            ContentType contentType;
            String header = httpRequest.getHeaders().getValue(HttpHeaders.CONTENT_TYPE);
            boolean isRequest =
                    httpRequest.getHttpMethod() == com.azure.core.http.HttpMethod.POST || httpRequest.getHttpMethod() == com.azure.core.http.HttpMethod.PUT || httpRequest.getHttpMethod() == com.azure.core.http.HttpMethod.PATCH || httpRequest.getHttpMethod() == com.azure.core.http.HttpMethod.DELETE;
            contentType = header != null ? ContentType.parse(header) : (isRequest ?
                    ContentType.APPLICATION_FORM_URLENCODED :
                    ContentType.APPLICATION_OCTET_STREAM);

            String contentLengthStr = httpRequest.getHeaders().getValue(HttpHeaders.CONTENT_LENGTH);
            long contentLength = contentLengthStr != null ? Long.parseLong(contentLengthStr) : -1;

            request.setEntity(new InputStreamEntity(isPipe, contentLength, contentType));
        }

        // The types of the Monos are different, but we ignore the results anyway (since we only care about the input stream) so this is fine.
        return before.flatMap(a -> Mono.create(monoSink -> {
            try {
                org.apache.hc.core5.http.HttpResponse response = httpClient.execute(request);
                monoSink.success(new HttpResponseWrapper(httpRequest, (ClassicHttpResponse) response));
            } catch (IOException e) {
                monoSink.error(e);
            }
        }));
    }

    // Implementation of the synchronous HttpClient
    @Override
    public IHttpResponse send(com.microsoft.aad.msal4j.HttpRequest httpRequest) throws Exception {
        HttpUriRequest request;

        // Translating the request

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
        // Translating the headers

        request.setHeaders(httpRequest.headers().entrySet().stream().map(h -> new BasicHeader(h.getKey(), h.getValue())).toArray(Header[]::new));

        ContentType contentType;
        String header = httpRequest.headerValue(HttpHeaders.CONTENT_TYPE);
        contentType = header != null ? ContentType.parse(header) : (httpRequest.httpMethod() == HttpMethod.GET ? ContentType.APPLICATION_OCTET_STREAM :
                ContentType.APPLICATION_FORM_URLENCODED);

        // Setting the request's body/entity
        String body = httpRequest.body();
        if (body != null) {
            request.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8), contentType));
        }

        return new HttpResponseWrapper((ClassicHttpResponse) httpClient.execute(request));
    }
}
