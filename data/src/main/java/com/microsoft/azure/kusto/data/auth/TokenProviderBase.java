package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;

import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TokenProviderBase {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;

    public TokenProviderBase(@NotNull String clusterUrl) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
    }

    public abstract String acquireAccessToken(@Nullable HttpClient httpClient) throws DataServiceException, DataClientException;
}
