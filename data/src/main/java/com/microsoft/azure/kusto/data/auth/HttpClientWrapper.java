package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;

import org.apache.http.Header;
import org.apache.http.HttpEntityEnclosingRequest;
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
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;

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
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest entityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            PipedOutputStream osPipe = new PipedOutputStream();
            PipedInputStream isPipe = null;
            try {
                isPipe = new PipedInputStream(osPipe);
            } catch (IOException ignored) {
                // This should never happen
            }

            // This creates the Mono (similar to a future) that will be used to write the body, we will have
            before = Mono.from(httpRequest.getBody().map(buf -> {
                try {
                    // Ignore the warning saying it's a blocking operation - since we are controlling both pipes, it shouldn't be
                    osPipe.write(buf.array());
                } catch (IOException e) {
                    // If this happens there's not much we can do here
                }
                return buf;
            }));

            entityEnclosingRequest.setEntity(new InputStreamEntity(isPipe));
        }

        // The types of the Monos are different, but we ignore the results anyway (since we only care about the input stream) so this is fine.
        return before.flatMap(a -> Mono.create(monoSink -> {
            try {
                org.apache.http.HttpResponse response = httpClient.execute(request);
                monoSink.success(new HttpResponseWrapper(httpRequest, response));
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

        // Setting the request's body/entity
        if (request instanceof HttpEntityEnclosingRequest) {
            HttpEntityEnclosingRequest entityEnclosingRequest = (HttpEntityEnclosingRequest) request;
            String body = httpRequest.body();
            entityEnclosingRequest.setEntity(new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8)));
        }

        return new HttpResponseWrapper(httpClient.execute(request));
    }
}
