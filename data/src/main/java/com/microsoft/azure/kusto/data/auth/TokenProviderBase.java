package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TokenProviderBase implements TraceableAttributes {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;
    protected final HttpClient httpClient;

    public TokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
        this.httpClient = httpClient;
    }

    public String acquireAccessToken() throws DataServiceException, DataClientException {
        initialize();
        // trace getToken
        return MonitoredActivity.invoke((SupplierTwoExceptions<String, DataServiceException, DataClientException>) this::acquireAccessTokenImpl,
                getAuthMethod().concat(".acquireAccessToken"), getTracingAttributes());
    }

    void initialize() throws DataClientException, DataServiceException {
    }

    protected abstract String acquireAccessTokenImpl() throws DataServiceException, DataClientException;

    protected abstract String getAuthMethod();

    @Override
    public Map<String, String> getTracingAttributes() {
        return new HashMap<>();
    }
}
