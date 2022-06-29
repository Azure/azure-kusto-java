// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hc.client5.http.classic.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class CloudDependentTokenProviderBase extends TokenProviderBase {
    private static final String ERROR_INVALID_SERVICE_RESOURCE_URL = "Error determining scope due to invalid Kusto Service Resource URL";
    protected final Set<String> scopes = new HashSet<>();
    private boolean initialized = false;

    CloudDependentTokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
    }

    synchronized void initialize() throws DataClientException, DataServiceException {
        if (initialized) {
            return;
        }

        initializeWithCloudInfo(CloudInfo.retrieveCloudInfoForCluster(clusterUrl));
        initialized = true;
    }

    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataClientException, DataServiceException {
        try {
            scopes.add(cloudInfo.determineScope());
        } catch (URISyntaxException e) {
            throw new DataServiceException(clusterUrl, ERROR_INVALID_SERVICE_RESOURCE_URL, e, true);
        }
    }

    @Override
    public String acquireAccessToken() throws DataServiceException, DataClientException {
        initialize();
        return acquireAccessTokenImpl();
    }

    protected abstract String acquireAccessTokenImpl() throws DataServiceException, DataClientException;
}
