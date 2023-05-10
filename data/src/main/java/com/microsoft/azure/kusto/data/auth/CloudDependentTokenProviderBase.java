// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.apache.http.client.HttpClient;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class CloudDependentTokenProviderBase extends TokenProviderBase implements TraceableAttributes {
    private static final String ERROR_INVALID_SERVICE_RESOURCE_URL = "Error determining scope due to invalid Kusto Service Resource URL";
    protected final Set<String> scopes = new HashSet<>();
    private boolean initialized = false;
    private CloudInfo cloudInfo;

    CloudDependentTokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
    }

    synchronized void initialize() throws DataClientException, DataServiceException {
        if (initialized) {
            return;
        }
        // trace retrieveCloudInfo
        cloudInfo = MonitoredActivity.invoke(
                (SupplierTwoExceptions<CloudInfo, DataClientException, DataServiceException>) () -> CloudInfo.retrieveCloudInfoForCluster(clusterUrl,
                        httpClient),
                "CloudDependentTokenProviderBase.retrieveCloudInfo", getTracingAttributes(new HashMap<>()));
        initializeWithCloudInfo(cloudInfo);
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
        return acquireAccessTokenInner();
    }

    @Override
    protected String acquireAccessTokenInner() throws DataServiceException, DataClientException {
        initialize();
        Map<String, String> attributes = cloudInfo != null ? new HashMap<>(Map.of("resource", cloudInfo.getKustoServiceResourceId())) : null;
        return MonitoredActivity.invoke((SupplierTwoExceptions<String, DataServiceException, DataClientException>) this::acquireAccessTokenImpl,
                getAuthMethod().concat(".acquireAccessToken"), attributes);
    }

    protected abstract String acquireAccessTokenImpl() throws DataServiceException, DataClientException;

    @Override
    public Map<String, String> getTracingAttributes(Map<String, String> attributes) {
        attributes.put("http.url", clusterUrl);
        return attributes;
    }
}
