package com.microsoft.azure.kusto.data.auth;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.ExponentialRetry;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.data.http.HttpClientFactory;
import com.microsoft.azure.kusto.data.http.HttpStatus;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import com.microsoft.azure.kusto.data.req.RequestUtils;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import reactor.util.retry.Retry;

public class CloudInfo implements TraceableAttributes, Serializable {
    private static final ConcurrentMap<String, Mono<CloudInfo>> cache = new ConcurrentHashMap<>();

    public static final String METADATA_ENDPOINT = "v1/rest/auth/metadata";
    public static final String DEFAULT_KUSTO_CLIENT_APP_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    public static final boolean DEFAULT_LOGIN_MFA_REQUIRED = false;
    public static final String DEFAULT_PUBLIC_LOGIN_URL = "https://login.microsoftonline.com";
    public static final String DEFAULT_REDIRECT_URI = "https://microsoft/kustoclient";
    public static final String DEFAULT_KUSTO_SERVICE_RESOURCE_ID = "https://kusto.kusto.windows.net";
    public static final String DEFAULT_FIRST_PARTY_AUTHORITY_URL = "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a";
    public static final CloudInfo DEFAULT_CLOUD = new CloudInfo(
            DEFAULT_LOGIN_MFA_REQUIRED,
            DEFAULT_PUBLIC_LOGIN_URL,
            DEFAULT_KUSTO_CLIENT_APP_ID,
            DEFAULT_REDIRECT_URI,
            DEFAULT_KUSTO_SERVICE_RESOURCE_ID,
            DEFAULT_FIRST_PARTY_AUTHORITY_URL);
    public static final String LOCALHOST = "http://localhost";
    private static final Duration CLOUD_INFO_TIMEOUT = Duration.ofSeconds(10);

    static {
        cache.put(LOCALHOST, Mono.just(DEFAULT_CLOUD));
    }

    private final boolean loginMfaRequired;
    private final String loginEndpoint;
    private final String kustoClientAppId;
    private final String kustoClientRedirectUri;
    private final String kustoServiceResourceId;
    private final String firstPartyAuthorityUrl;
    private static final int ATTEMPT_COUNT = 3;
    private static final Retry RETRY_CONFIG = new ExponentialRetry<>(ATTEMPT_COUNT).retry(null);

    public CloudInfo(boolean loginMfaRequired, String loginEndpoint, String kustoClientAppId, String kustoClientRedirectUri, String kustoServiceResourceId,
            String firstPartyAuthorityUrl) {
        this.loginMfaRequired = loginMfaRequired;
        this.loginEndpoint = loginEndpoint;
        this.kustoClientAppId = kustoClientAppId;
        this.kustoClientRedirectUri = kustoClientRedirectUri;
        this.kustoServiceResourceId = kustoServiceResourceId;
        this.firstPartyAuthorityUrl = firstPartyAuthorityUrl;
    }

    public static void manuallyAddToCache(String clusterUrl, Mono<CloudInfo> cloudInfoMono) throws URISyntaxException {
        cache.putIfAbsent(UriUtils.setPathForUri(clusterUrl, ""), cloudInfoMono);
    }

    public static CloudInfo retrieveCloudInfoForCluster(String clusterUrl) {
        return retrieveCloudInfoForClusterAsync(clusterUrl, null).block();
    }

    public static Mono<CloudInfo> retrieveCloudInfoForClusterAsync(String clusterUrl, @Nullable HttpClient givenHttpClient) {

        // We ensure that if multiple threads request the cloud info for the same cluster url, only one http call will be made
        // for all corresponding threads
        try {
            Tuple2<String, String> clusterUrls = Tuples.of(
                    UriUtils.setPathForUri(clusterUrl, ""), // Cluster endpoint
                    UriUtils.setPathForUri(clusterUrl, METADATA_ENDPOINT) // Metadata endpoint is always on the root of the cluster
            );
            return cache.computeIfAbsent(clusterUrls.getT1(), key -> fetchCloudInfoAsync(clusterUrls, givenHttpClient)
                    .retryWhen(RETRY_CONFIG)
                    .onErrorMap(e -> ExceptionUtils.unwrapCloudInfoException(clusterUrls.getT1(), e)));
        } catch (URISyntaxException e) {
            throw new DataServiceException(clusterUrl,
                    "URISyntaxException when trying to retrieve cluster metadata: " + e.getMessage(), e, true);
        }
    }

    private static Mono<CloudInfo> fetchCloudInfoAsync(Tuple2<String, String> clusterUrls, @Nullable HttpClient givenHttpClient) {
        HttpClient localHttpClient = givenHttpClient == null ? HttpClientFactory.create(null) : givenHttpClient;
        HttpRequest request = new HttpRequest(HttpMethod.GET, clusterUrls.getT2());
        request.setHeader(HttpHeaderName.ACCEPT_ENCODING, "gzip,deflate");
        request.setHeader(HttpHeaderName.ACCEPT, "application/json");

        return MonitoredActivity.wrap(localHttpClient.send(request, RequestUtils.contextWithTimeout(CLOUD_INFO_TIMEOUT)),
                "CloudInfo.httpCall")
                .flatMap(response -> getCloudInfo(response, clusterUrls.getT1()))
                .doFinally(ignore -> {
                    if (givenHttpClient == null && localHttpClient instanceof Closeable) {
                        try {
                            ((Closeable) localHttpClient).close();
                        } catch (IOException ignore1) {
                        }
                    }
                });
    }

    private static Mono<CloudInfo> getCloudInfo(HttpResponse response, String clusterUrl) {
        int statusCode = response.getStatusCode();
        return Utils.getResponseBody(response)
                .map(content -> {
                    if (statusCode == HttpStatus.OK) {
                        if (content.isEmpty() || content.equals("{}")) {
                            throw new DataServiceException(clusterUrl, "Error in metadata endpoint, received no data", true);
                        }
                        return parseCloudInfo(content);
                    } else if (statusCode == HttpStatus.NOT_FOUND) {
                        return DEFAULT_CLOUD;
                    } else {
                        String errorFromResponse = content;
                        if (errorFromResponse.isEmpty()) {
                            // Fixme: Missing reason phrase to add to exception. Potentially want to use an enum.
                            errorFromResponse = "";
                        }
                        throw new DataServiceException(clusterUrl, "Error in metadata endpoint, got code: " + statusCode +
                                "\nWith error: " + errorFromResponse, statusCode != HttpStatus.TOO_MANY_REQS);
                    }
                });
    }

    private static CloudInfo parseCloudInfo(String content) {
        try {
            ObjectMapper objectMapper = Utils.getObjectMapper();
            JsonNode jsonObject = objectMapper.readTree(content);
            JsonNode innerObject = jsonObject.has("AzureAD") ? jsonObject.get("AzureAD") : null;
            if (innerObject == null) {
                return DEFAULT_CLOUD;
            } else {
                return new CloudInfo(
                        innerObject.has("LoginMfaRequired") && innerObject.get("LoginMfaRequired").asBoolean(),
                        innerObject.has("LoginEndpoint") ? innerObject.get("LoginEndpoint").asText() : "",
                        innerObject.has("KustoClientAppId") ? innerObject.get("KustoClientAppId").asText() : "",
                        innerObject.has("KustoClientRedirectUri") ? innerObject.get("KustoClientRedirectUri").asText() : "",
                        innerObject.has("KustoServiceResourceId") ? innerObject.get("KustoServiceResourceId").asText() : "",
                        innerObject.has("FirstPartyAuthorityUrl") ? innerObject.get("FirstPartyAuthorityUrl").asText() : "");
            }
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CloudInfo cloudInfo = (CloudInfo) o;

        return loginMfaRequired == cloudInfo.loginMfaRequired
                && Objects.equals(loginEndpoint, cloudInfo.loginEndpoint)
                && Objects.equals(kustoClientAppId, cloudInfo.kustoClientAppId)
                && Objects.equals(kustoClientRedirectUri, cloudInfo.kustoClientRedirectUri)
                && Objects.equals(kustoServiceResourceId, cloudInfo.kustoServiceResourceId)
                && Objects.equals(firstPartyAuthorityUrl, cloudInfo.firstPartyAuthorityUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(loginMfaRequired, loginEndpoint, kustoClientAppId, kustoClientRedirectUri, kustoServiceResourceId, firstPartyAuthorityUrl);
    }

    public boolean isLoginMfaRequired() {
        return loginMfaRequired;
    }

    public String getLoginEndpoint() {
        return loginEndpoint;
    }

    public String getKustoClientAppId() {
        return kustoClientAppId;
    }

    public String getKustoClientRedirectUri() {
        return kustoClientRedirectUri;
    }

    public String getKustoServiceResourceId() {
        return kustoServiceResourceId;
    }

    @Override
    public Map<String, String> getTracingAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("resource", kustoServiceResourceId);
        return attributes;
    }

    public String getFirstPartyAuthorityUrl() {
        return firstPartyAuthorityUrl;
    }

    public String determineScope() throws URISyntaxException {
        String resourceUrl = getKustoServiceResourceId();
        if (isLoginMfaRequired()) {
            resourceUrl = resourceUrl.replace(".kusto.", ".kustomfa.");
        }

        resourceUrl = StringUtils.appendIfMissing(resourceUrl, "/");
        return resourceUrl + ".default";
    }

}
