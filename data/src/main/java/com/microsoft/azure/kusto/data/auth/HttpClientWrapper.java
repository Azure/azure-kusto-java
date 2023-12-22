package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.*;
import com.azure.core.util.BinaryData;
import com.azure.core.util.Context;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;

import reactor.core.publisher.Mono;

import java.util.*;

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
        HttpRequest request = new HttpRequest(method, httpRequest.url(), new HttpHeaders(httpRequest.headers()),
                BinaryData.fromString(httpRequest.body()));

        // TODO Ohad - But we are in Sync mode - so its ok to block, maybe take code from this reference :
        // https://github.com/Azure/azure-sdk-for-java/blob/6732ed81c3364bc418c4c80b8d781e6b1f741536/sdk/identity/azure-identity/src/main/java/com/azure/identity/implementation/HttpPipelineAdapter.java#L18
        com.microsoft.aad.msal4j.HttpResponse msalResponse;
        try (HttpResponse response = httpClient.sendSync(request, Context.NONE)) {

            msalResponse = new com.microsoft.aad.msal4j.HttpResponse();
            msalResponse.statusCode(response.getStatusCode());
            // Todo: Cole - String body = response.getBodyAsBinaryData().toString();
            msalResponse.body(response.getBodyAsString().block());
        }

        Map<String, List<String>> headers = new HashMap<>();

        // Java 11 will make this much more concise
        httpRequest.headers().entrySet().stream()
                .filter(entry -> isNotContentLength(entry.getKey()))
                .forEach(entry -> {
                    List<String> values = new ArrayList<>();
                    values.add(entry.getValue());
                    headers.put(entry.getKey(), values);
                });

        msalResponse.addHeaders(headers);
        return msalResponse;
    }
}
