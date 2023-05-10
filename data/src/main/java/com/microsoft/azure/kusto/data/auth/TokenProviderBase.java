package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;

import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
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

    public String acquireAccessToken() throws DataServiceException, DataClientException {

        // trace GetToken
        return MonitoredActivity.invoke((SupplierTwoExceptions<String, DataServiceException, DataClientException>) this::acquireAccessTokenInner,
                getAuthMethod().concat(".acquireAccessToken"));
    }

    protected abstract String acquireAccessTokenInner() throws DataServiceException, DataClientException;

    protected abstract String getAuthMethod();
}
