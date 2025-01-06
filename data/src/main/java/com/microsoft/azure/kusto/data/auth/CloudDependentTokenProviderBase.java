// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.azure.core.http.HttpClient;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;

import reactor.core.publisher.Mono;

public abstract class CloudDependentTokenProviderBase extends TokenProviderBase {
    private static final String ERROR_INVALID_SERVICE_RESOURCE_URL = "Error determining scope due to invalid Kusto Service Resource URL";
    protected final Set<String> scopes = new HashSet<>();
    private boolean initialized = false;
    private CloudInfo cloudInfo;

    CloudDependentTokenProviderBase(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
    }

    @Override
    final Mono<Void> initialize() {
        return Mono.defer(() -> {
            if (initialized) {
                return Mono.empty();
            }

            // trace retrieveCloudInfo
            return MonitoredActivity.wrap(
                    CloudInfo.retrieveCloudInfoForClusterAsync(clusterUrl, httpClient),
                    "CloudDependentTokenProviderBase.retrieveCloudInfo", getTracingAttributes())
                    .doOnNext(cloudInfoResult -> {
                        this.cloudInfo = cloudInfoResult;
                        initializeWithCloudInfo(cloudInfoResult);
                        this.initialized = true;
                    })
                    .then();
        });
    }

    protected void initializeWithCloudInfo(CloudInfo cloudInfo) {
        try {
            scopes.add(cloudInfo.determineScope());
        } catch (URISyntaxException e) {
            throw new DataServiceException(clusterUrl, ERROR_INVALID_SERVICE_RESOURCE_URL, e, true);
        }
    }

    @Override
    public Map<String, String> getTracingAttributes() {
        Map<String, String> attributes = super.getTracingAttributes();
        if (cloudInfo != null) {
            attributes.putAll(cloudInfo.getTracingAttributes());
        }
        attributes.put("http.url", clusterUrl);
        return attributes;
    }
}
