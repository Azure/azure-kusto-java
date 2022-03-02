// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public abstract class CloudDependantTokenProviderBase extends TokenProviderBase {
    private static final String ERROR_INVALID_SERVICE_RESOURCE_URL =
            "Error determining scope due to invalid Kusto Service Resource URL";
    protected final Set<String> scopes = new HashSet<>();
    protected CloudInfo cloudInfo = null;

    CloudDependantTokenProviderBase(@NotNull String clusterUrl) throws URISyntaxException {
        super(clusterUrl);
    }

    protected void onCloudInfoInitialized() throws DataClientException, DataServiceException {
        try {
            scopes.add(cloudInfo.determineScope());
        } catch (URISyntaxException e) {
            throw new DataServiceException(clusterUrl, ERROR_INVALID_SERVICE_RESOURCE_URL, e, true);
        }
    }

    protected void initializeCloudInfo() throws DataClientException, DataServiceException {
        if (cloudInfo != null) {
            return;
        }
        synchronized (this) {
            if (cloudInfo != null) {
                return;
            }

            cloudInfo = CloudInfo.retrieveCloudInfoForCluster(clusterUrl);
            onCloudInfoInitialized();
        }
    }

    @Override
    public String acquireAccessToken() throws DataServiceException, DataClientException {
        initializeCloudInfo();
        return acquireAccessTokenAfterCloudInfo();
    }

    public abstract String acquireAccessTokenAfterCloudInfo() throws DataServiceException, DataClientException;
}
