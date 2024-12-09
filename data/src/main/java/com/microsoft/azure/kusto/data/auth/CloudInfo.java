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

import reactor.core.publisher.Mono;

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
    private static final ExponentialRetry exponentialRetryTemplate = new ExponentialRetry<>(ATTEMPT_COUNT);

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
        synchronized (cache) {
            cache.put(UriUtils.setPathForUri(clusterUrl, ""), cloudInfoMono);
        }
    }

    public static CloudInfo retrieveCloudInfoForCluster(String clusterUrl) {
        return retrieveCloudInfoForClusterAsync(clusterUrl, null).block();
    }

    public static Mono<CloudInfo> retrieveCloudInfoForClusterAsync(String clusterUrl, @Nullable HttpClient givenHttpClient) {

        // Ensure that if multiple threads request the cloud info for the same cluster url, only one http call will be made
        // for all corresponding threads
        return Mono.fromCallable(() -> UriUtils.setPathForUri(clusterUrl, ""))
                .flatMap(url -> cache.computeIfAbsent(url, key -> fetchCloudInfoAsync(url, givenHttpClient)
                        .retryWhen(new ExponentialRetry<>(exponentialRetryTemplate).retry())
                        .onErrorMap(e -> ExceptionUtils.unwrapCloudInfoException(url, e))));
    }

    private static Mono<CloudInfo> fetchCloudInfoAsync(String clusterUrl, @Nullable HttpClient givenHttpClient) {
        HttpClient localHttpClient = givenHttpClient == null ? HttpClientFactory.create(null) : givenHttpClient;
        return Mono.using(
                () -> localHttpClient,
                client -> fetchCloudInfo(localHttpClient, clusterUrl),
                client -> closeHttpClient(localHttpClient, givenHttpClient))
                .onErrorMap(e -> ExceptionUtils.unwrapCloudInfoException(clusterUrl, e));
    }

    private static Mono<CloudInfo> fetchCloudInfo(HttpClient localHttpClient, String clusterUrl) {
        return sendRequest(localHttpClient, clusterUrl)
                .flatMap(response -> getCloudInfo(response, clusterUrl))
                .onErrorMap(e -> ExceptionUtils.unwrapCloudInfoException(clusterUrl, e));
    }

    private static Mono<HttpResponse> sendRequest(HttpClient localHttpClient, String clusterUrl) {
        return Mono.fromCallable(() -> {
            HttpRequest request = new HttpRequest(HttpMethod.GET, UriUtils.appendPathToUri(clusterUrl, METADATA_ENDPOINT));
            request.setHeader(HttpHeaderName.ACCEPT_ENCODING, "gzip,deflate");
            request.setHeader(HttpHeaderName.ACCEPT, "application/json");
            return request;
        })
                .flatMap(httpRequest -> MonitoredActivity.wrap(localHttpClient.send(httpRequest, RequestUtils.contextWithTimeout(CLOUD_INFO_TIMEOUT)),
                        "CloudInfo.httpCall"));
    }

    private static Mono<CloudInfo> getCloudInfo(HttpResponse response, String clusterUrl) {
        int statusCode = response.getStatusCode();
        return response.getBodyAsByteArray()
                .flatMap(bodyAsBinaryData -> {
                    if (statusCode == HttpStatus.OK) {
                        return parseCloudInfoSafely(response, bodyAsBinaryData, clusterUrl);
                    } else if (statusCode == HttpStatus.NOT_FOUND) {
                        return Mono.just(DEFAULT_CLOUD);
                    } else {
                        String errorFromResponse = new String(bodyAsBinaryData);
                        if (errorFromResponse.isEmpty()) {
                            // Fixme: Missing reason phrase to add to exception. Potentially want to use an enum.
                            errorFromResponse = "";
                        }
                        return Mono.error(new DataServiceException(clusterUrl, "Error in metadata endpoint, got code: " + statusCode +
                                "\nWith error: " + errorFromResponse, statusCode != HttpStatus.TOO_MANY_REQS));
                    }
                });
    }

    private static Mono<CloudInfo> parseCloudInfoSafely(HttpResponse response, byte[] bodyAsBinaryData, String clusterUrl) {
        try {
            String content = Utils.getContentAsString(response, bodyAsBinaryData);
            if (content.isEmpty() || content.equals("{}")) {
                return Mono.error(new DataServiceException(clusterUrl, "Error in metadata endpoint, received no data", true));
            }

            return Mono.just(parseCloudInfo(content));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private static Mono<Void> closeHttpClient(HttpClient localHttpClient, @Nullable HttpClient givenHttpClient) {
        if (givenHttpClient == null && localHttpClient instanceof Closeable) {
            try {
                ((Closeable) localHttpClient).close();
            } catch (IOException ex) {
                return Mono.error(ex);
            }
        }
        return Mono.empty();
    }

    private static CloudInfo parseCloudInfo(String content) throws JsonProcessingException {
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
