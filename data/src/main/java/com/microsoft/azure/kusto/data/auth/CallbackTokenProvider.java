// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import org.apache.hc.client5.http.classic.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class CallbackTokenProvider extends TokenProviderBase {
    private final CallbackTokenProviderFunction tokenProvider;

    CallbackTokenProvider(@NotNull String clusterUrl, @NotNull Callable<String> tokenProvider) throws URISyntaxException {
        super(clusterUrl, null);
        this.tokenProvider = (httpClient) -> tokenProvider.call();
    }

    CallbackTokenProvider(@NotNull String clusterUrl, @NotNull CallbackTokenProviderFunction tokenProvider,
            @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.tokenProvider = tokenProvider;
    }

    @Override
    public String acquireAccessToken() throws DataClientException {
        try {
            return tokenProvider.apply(httpClient);
        } catch (Exception e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }
    }
}
