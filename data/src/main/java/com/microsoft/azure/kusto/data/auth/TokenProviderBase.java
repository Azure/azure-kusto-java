// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TokenProviderBase {
    protected static final String ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN = "Error acquiring ApplicationAccessToken";
    private static final String ERROR_INVALID_SERVICE_RESOURCE_URL =
            "Error determining scope due to invalid Kusto Service Resource URL";
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;
    protected CloudInfo cloudInfo = null;

    TokenProviderBase(@NotNull String clusterUrl) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
    }

    protected void initializeCloudInfo() throws DataServiceException {
        if (cloudInfo != null) {
            return;
        }
        synchronized (this) {
            if (cloudInfo != null) {
                return;
            }

            cloudInfo = CloudInfo.retrieveCloudInfoForCluster(clusterUrl);
        }
    }

    protected String determineScope() throws DataServiceException {
        String resourceUrl = cloudInfo.getKustoServiceResourceId();
        if (cloudInfo.isLoginMfaRequired()) {
            resourceUrl = resourceUrl.replace(".kusto.", ".kustomfa.");
        }

        String scope;
        try {
            scope = UriUtils.setPathForUri(resourceUrl, ".default");
        } catch (URISyntaxException e) {
            throw new DataServiceException(clusterUrl, ERROR_INVALID_SERVICE_RESOURCE_URL, e, true);
        }
        return scope;
    }

    public abstract String acquireAccessToken() throws DataServiceException, DataClientException;
}
