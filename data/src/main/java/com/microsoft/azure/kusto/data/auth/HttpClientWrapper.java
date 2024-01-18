package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;
import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * This class wraps both of the azure http client interfaces - IHttpClient and HttpClient to use our apache http client.
 * These interfaces are required by the azure authentication classes - IHttpClient for Managed Identity, and HttpClient for the rest.
 * HttpClient is synchronous, so the implementation is straight-forward.
 * IHttpClient is asynchronous, so we need to be more clever about integrating it with the synchronous apache client.
 */
public class HttpClientWrapper implements com.azure.core.http.HttpClient, IHttpClient {

    private static final Executor EXECUTOR = Executors.newCachedThreadPool();
    private final HttpClient httpClient;

    public HttpClientWrapper(HttpClient httpClient) {
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

        request.setHeaders(httpRequest.getHeaders().stream().filter(h -> isNotContentLength(h.getName())).map(h -> new BasicHeader(h.getName(), h.getValue()))
                .toArray(Header[]::new));

        // Setting the request's body/entity

        // This empty operation
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest entityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            try (PipedOutputStream osPipe = new PipedOutputStream(); PipedInputStream isPipe = new PipedInputStream(osPipe)) {
                EXECUTOR.execute(() -> httpRequest.getBody().publishOn(Schedulers.boundedElastic()).map(buf -> {
                    try {
                        if (buf.hasArray()) {
                            osPipe.write(buf.array(), buf.position(), buf.remaining());
                        } else {
                            byte[] bytes = new byte[buf.remaining()];
                            buf.get(bytes);
                            osPipe.write(bytes);
                        }
                    } catch (IOException e) {
                        return false;
                    }
                    return true;
                }).blockLast());

                String contentLength = httpRequest.getHeaders().getValue("Content-Length");
                String contentType = httpRequest.getHeaders().getValue("Content-Type");
                entityEnclosingRequest.setEntity(new InputStreamEntity(isPipe, contentLength == null ? -1 : Long.parseLong(contentLength),
                        contentType == null ? null : ContentType.parse(contentType)));
            } catch (IOException e) {
                // This should never happen
            }
        }

        // The types of the Monos are different, but we ignore the results anyway (since we only care about the input stream) so this is fine.
        return Mono.create(monoSink -> {
            try {
                org.apache.http.HttpResponse response = httpClient.execute(request);
                monoSink.success(new HttpResponseWrapper(httpRequest, response));
            } catch (IOException e) {
                monoSink.error(e);
            }
        });
    }

    private static boolean isNotContentLength(String name) {
        return !name.equalsIgnoreCase("content-length");
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

        request.setHeaders(httpRequest.headers().entrySet().stream().filter(h -> isNotContentLength(h.getKey()))
                .map(h -> new BasicHeader(h.getKey(), h.getValue())).toArray(Header[]::new));

        // Setting the request's body/entity
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest entityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            String body = httpRequest.body();
            entityEnclosingRequest.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        }

        return new HttpResponseWrapper(httpClient.execute(request));
    }
}
