package com.microsoft.azure.kusto.data.auth;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.Map;

import com.microsoft.azure.kusto.data.instrumentation.DistributedTracing;
import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TokenProviderBase {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;
    protected final HttpClient httpClient;

    public TokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
        this.httpClient = httpClient;
    }
    public String acquireAccessToken() throws DataServiceException, DataClientException{

        // trace GetToken
        try (DistributedTracing.Span span = DistributedTracing.startSpan(getAuthMethod().concat(".acquireAccessToken"), Context.NONE, ProcessKind.PROCESS, null)) {
            try {
                return acquireAccessTokenInner();
            } catch (DataServiceException | DataClientException e) {
                span.addException(e);
                throw e;
            }
        }
    }

    abstract String acquireAccessTokenInner() throws DataServiceException, DataClientException;

    protected String getAuthMethod() {
        return "TokenProviderBase";
    }
}
