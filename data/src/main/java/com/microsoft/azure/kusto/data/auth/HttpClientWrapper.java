package com.microsoft.azure.kusto.data.auth;

import com.azure.core.http.*;
import com.azure.core.util.Context;
import com.azure.core.util.CoreUtils;
import com.microsoft.aad.msal4j.IHttpClient;
import com.microsoft.aad.msal4j.IHttpResponse;

import java.util.stream.Collectors;

/**
 * This class wraps an Azure Core HttpClient to be used as an MSAL IHttpClient.
 */
public class HttpClientWrapper implements IHttpClient {

    private final HttpClient httpClient;

    public HttpClientWrapper(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    // Implementation of the synchronous HttpClient
    @Override
    public IHttpResponse send(com.microsoft.aad.msal4j.HttpRequest httpRequest) { // TODO: does this need to be changed?
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
