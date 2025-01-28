package com.microsoft.azure.kusto.data.http;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.util.BinaryData;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.req.KustoRequest;

public class HttpRequestBuilder {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // TODO - maybe save this in a resource
    private static final String KUSTO_API_VERSION = "2019-02-13";
    private static final String CLIENT_VERSION_HEADER = "x-ms-client-version";
    private static final String APP_HEADER = "x-ms-app";
    private static final String USER_HEADER = "x-ms-user";

    private final HttpRequest request;

    public static HttpRequestBuilder fromExistingRequest(HttpRequest request) {
        return new HttpRequestBuilder(request);
    }

    public static HttpRequestBuilder newPost(String url) {
        return new HttpRequestBuilder(HttpMethod.POST, url);
    }

    private HttpRequestBuilder(HttpRequest request) {
        this.request = request;
    }

    public HttpRequestBuilder(HttpMethod method, String url) {
        URL cleanURL = parseURLString(url);
        request = new HttpRequest(method, cleanURL);
    }

    public HttpRequestBuilder createCommandPayload(KustoRequest kr) {
        ObjectNode json = Utils.getObjectMapper().createObjectNode()
                .put("db", kr.getDatabase())
                .put("csl", kr.getCommand());

        if (kr.getProperties() != null) {
            json.put("properties", kr.getProperties().toString());
        }

        request.setBody(json.toString());

        // When executing a query/command, we always add content type
        // Updated to remove Fed True from command headers per PR #342
        request.setHeader(HttpHeaderName.CONTENT_TYPE, "application/json; charset=utf-8");

        return this;
    }

    public HttpRequestBuilder withBody(BinaryData body) {
        request.setBody(body);
        return this;
    }

    public HttpRequestBuilder withAuthorization(String value) {
        if (value != null) {
            request.setHeader(HttpHeaderName.AUTHORIZATION, value);
        }
        return this;
    }

    public HttpRequestBuilder withContentEncoding(String contentEncoding) {
        if (contentEncoding != null) {
            request.setHeader(HttpHeaderName.CONTENT_ENCODING, contentEncoding);
        }
        return this;
    }

    public HttpRequestBuilder withContentType(String contentType) {
        if (contentType != null) {
            request.setHeader(HttpHeaderName.CONTENT_TYPE, contentType);
        }
        return this;
    }

    public HttpRequestBuilder withHeaders(Map<String, String> headers) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            request.setHeader(HttpHeaderName.fromString(entry.getKey()), entry.getValue());
        }
        return this;
    }

    public HttpRequestBuilder withTracing(HttpTracing tracing) {
        return this.withHeaders(getTracingHeaders(tracing));
    }

    public HttpRequestBuilder withURL(String url) {
        URL cleanURL = parseURLString(url);
        request.setUrl(cleanURL);
        return this;
    }

    public HttpRequest build() {
        // If has authorization header, ensure it is not sent over insecure channel
        boolean hasAuth = request.getHeaders().stream().anyMatch(h -> h.getName().equalsIgnoreCase(HttpHeaderName.AUTHORIZATION.toString()));
        if (hasAuth) {
            URL url = request.getUrl();
            boolean isHttp = url.getProtocol().equalsIgnoreCase("http");
            boolean isLocalhost = url.getHost().equalsIgnoreCase(CloudInfo.LOCALHOST);

            if (isHttp) {
                if (isLocalhost) {
                    log.warn("Sending security token to localhost over an unencrypted channel (http://)");
                } else {
                    throw new DataClientException(url.toString(), "Cannot forward security token to a remote service over an unencrypted channel (http://)");
                }
            }
        }

        // Set global headers that get added to all requests
        request.setHeader(HttpHeaderName.ACCEPT_ENCODING, "gzip,deflate");
        request.setHeader(HttpHeaderName.ACCEPT, "application/json");

        // Removed content type from this method because the request should already have a type that is not always json
        request.setHeader(HttpHeaderName.fromString("x-ms-version"), KUSTO_API_VERSION);

        return request;
    }

    @NotNull
    private static URL parseURLString(String url) {
        try {
            // By nature of the try/catch only valid URLs pass through this function
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new DataClientException(url, "Error parsing target URL in post request:" + e.getMessage(), e);
        }
    }

    private Map<String, String> getTracingHeaders(HttpTracing tracing) {

        Map<String, String> headers = new HashMap<>();

        String version = tracing.getClientDetails().getClientVersionForTracing();
        if (StringUtils.isNotBlank(version)) {
            headers.put(CLIENT_VERSION_HEADER, version);
        }

        String app = (tracing.getProperties() == null || tracing.getProperties().getApplication() == null)
                ? tracing.getClientDetails().getApplicationForTracing()
                : tracing.getProperties().getApplication();
        if (StringUtils.isNotBlank(app)) {
            headers.put(APP_HEADER, app);
        }

        String user = (tracing.getProperties() == null || tracing.getProperties().getUser() == null) ? tracing.getClientDetails().getUserNameForTracing()
                : tracing.getProperties().getUser();
        if (StringUtils.isNotBlank(user)) {
            headers.put(USER_HEADER, user);
        }

        String clientRequestId;
        if (tracing.getProperties() != null && StringUtils.isNotBlank(tracing.getProperties().getClientRequestId())) {
            clientRequestId = tracing.getProperties().getClientRequestId();
        } else {
            clientRequestId = String.format("%s;%s", tracing.getClientRequestIdPrefix(), UUID.randomUUID());
        }

        headers.put("x-ms-client-request-id", clientRequestId);

        // Configures Keep-Alive on all requests traced
        headers.put("Connection", "Keep-Alive");

        // replace non-ascii characters in header values with '?'
        headers.replaceAll((_i, v) -> v == null ? null : v.replaceAll("[^\\x00-\\x7F]", "?"));
        return headers;
    }

}
