// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import com.microsoft.azure.kusto.data.instrumentation.DistributedTracing;
import org.apache.http.client.HttpClient;

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
        // trace retrieveCloudInfo
        try (DistributedTracing.Span span = DistributedTracing.startSpan("CloudDependentTokenProviderBase.retrieveCloudInfo", Context.NONE, ProcessKind.PROCESS, null)) {
            try {
                initializeWithCloudInfo(CloudInfo.retrieveCloudInfoForCluster(clusterUrl, httpClient));
            } catch (DataClientException | DataServiceException e) {
                span.addException(e);
                throw e;
            }
        }
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
    String acquireAccessTokenInner() throws DataServiceException, DataClientException {
        initialize();
        return acquireAccessTokenImpl();
    }

    protected abstract String acquireAccessTokenImpl() throws DataServiceException, DataClientException;
}
