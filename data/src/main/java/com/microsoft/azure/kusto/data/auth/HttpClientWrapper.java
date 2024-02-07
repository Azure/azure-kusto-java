package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.*;
import com.azure.core.util.Context;
import com.azure.core.util.CoreUtils;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;

import reactor.core.publisher.Mono;

import java.util.stream.Collectors;

/**
 * This class wraps both of the azure http client interfaces - IHttpClient and HttpClient to use our apache http client.
 * These interfaces are required by the azure authentication classes - IHttpClient for Managed Identity, and HttpClient for the rest.
 * HttpClient is synchronous, so the implementation is straight-forward.
 * IHttpClient is asynchronous, so we need to be more clever about integrating it with the synchronous apache client.
 */
public class HttpClientWrapper implements HttpClient, IHttpClient {

    private final HttpClient httpClient;

    public HttpClientWrapper(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    // Implementation of the asynchronous IHttpClient
    @Override
    public Mono<HttpResponse> send(HttpRequest httpRequest) {
        return httpClient.send(httpRequest);
    }

    private static boolean isNotContentLength(String name) {
        return !name.equalsIgnoreCase("content-length");
    }

    // Implementation of the synchronous HttpClient
    @Override
    public IHttpResponse send(com.microsoft.aad.msal4j.HttpRequest httpRequest) {
        HttpMethod method;
        switch (httpRequest.httpMethod()) {
            case GET:
                method = HttpMethod.GET;
                break;
            case POST:
                method = HttpMethod.POST;
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + httpRequest.httpMethod());
        }

        // Generate an azure core HttpRequest from the existing msal4j HttpRequest
        HttpRequest request = new HttpRequest(method, httpRequest.url(), new HttpHeaders(httpRequest.headers()));
        if (!CoreUtils.isNullOrEmpty(httpRequest.body())) {
            request.setBody(httpRequest.body());
        }

        try (HttpResponse response = httpClient.sendSync(request, Context.NONE)) {
            com.microsoft.aad.msal4j.HttpResponse msalResponse = new com.microsoft.aad.msal4j.HttpResponse();
            msalResponse.statusCode(response.getStatusCode());
            msalResponse.body(response.getBodyAsBinaryData().toString());
            msalResponse.addHeaders(response.getHeaders().stream().collect(Collectors.toMap(HttpHeader::getName,
                    HttpHeader::getValuesList)));
            return msalResponse;
        }
    }
}
