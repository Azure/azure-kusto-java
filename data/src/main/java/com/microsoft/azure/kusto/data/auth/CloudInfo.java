package com.microsoft.azure.kusto.data.auth;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.UriUtils;

import com.microsoft.azure.kusto.data.Utils;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.instrumentation.DistributedTracing;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CloudInfo {
    private static final Map<String, CloudInfo> cache = new HashMap<>();

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

    static {
        cache.put(LOCALHOST, DEFAULT_CLOUD);
    }

    private final boolean loginMfaRequired;
    private final String loginEndpoint;
    private final String kustoClientAppId;
    private final String kustoClientRedirectUri;
    private final String kustoServiceResourceId;
    private final String firstPartyAuthorityUrl;

    public CloudInfo(boolean loginMfaRequired, String loginEndpoint, String kustoClientAppId, String kustoClientRedirectUri, String kustoServiceResourceId,
            String firstPartyAuthorityUrl) {
        this.loginMfaRequired = loginMfaRequired;
        this.loginEndpoint = loginEndpoint;
        this.kustoClientAppId = kustoClientAppId;
        this.kustoClientRedirectUri = kustoClientRedirectUri;
        this.kustoServiceResourceId = kustoServiceResourceId;
        this.firstPartyAuthorityUrl = firstPartyAuthorityUrl;
    }

    public static void manuallyAddToCache(String clusterUrl, CloudInfo cloudInfo) throws URISyntaxException {
        synchronized (cache) {
            cache.put(UriUtils.setPathForUri(clusterUrl, ""), cloudInfo);
        }
    }

    public static CloudInfo retrieveCloudInfoForCluster(String clusterUrl) throws DataServiceException {
        return retrieveCloudInfoForCluster(clusterUrl, null);
    }

    public static CloudInfo retrieveCloudInfoForCluster(String clusterUrl,
            @Nullable HttpClient givenHttpClient)
            throws DataServiceException {
        synchronized (cache) {
            CloudInfo cloudInfo;
            try {
                cloudInfo = cache.get(UriUtils.setPathForUri(clusterUrl, ""));
            } catch (URISyntaxException ex) {
                throw new DataServiceException(clusterUrl, "Error in metadata endpoint, cluster uri invalid", ex, true);
            }
            if (cloudInfo != null) {
                return cloudInfo;
            }

            CloudInfo result;

            try {
                HttpClient localHttpClient = givenHttpClient == null ? HttpClients.createSystem() : givenHttpClient;
                try {
                    HttpGet request = new HttpGet(UriUtils.setPathForUri(clusterUrl, METADATA_ENDPOINT));
                    request.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip,deflate");
                    request.addHeader(HttpHeaders.ACCEPT, "application/json");

                    // trace CloudInfo.httpCall
                    HttpResponse response;
                    try (DistributedTracing.Span span = DistributedTracing.startSpan("CloudInfo.httpCall", Context.NONE, ProcessKind.PROCESS, null)) {
                        try {
                            response = localHttpClient.execute(request);
                        } catch (IOException e) {
                            span.addException(e);
                            throw e;
                        }
                    }
                    try {
                        int statusCode = response.getStatusLine().getStatusCode();
                        if (statusCode == 200) {
                            String content = EntityUtils.toString(response.getEntity());
                            if (content == null || content.equals("") || content.equals("{}")) {
                                throw new DataServiceException(clusterUrl, "Error in metadata endpoint, received no data", true);
                            }
                            result = parseCloudInfo(content);
                        } else if (statusCode == 404) {
                            result = DEFAULT_CLOUD;
                        } else {
                            String errorFromResponse = EntityUtils.toString(response.getEntity());
                            throw new DataServiceException(clusterUrl,
                                    "Error in metadata endpoint, got code: " + statusCode + "\nWith error: " + errorFromResponse, true);
                        }
                    } finally {
                        if (response instanceof Closeable) {
                            ((Closeable) response).close();
                        }
                    }
                } finally {
                    if (givenHttpClient == null && localHttpClient != null) {
                        ((Closeable) localHttpClient).close();
                    }
                }
            } catch (IOException | URISyntaxException ex) {
                throw new DataServiceException(clusterUrl, "IOError when trying to retrieve CloudInfo", ex, true);
            }
            cache.put(clusterUrl, result);
            return result;
        }
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
