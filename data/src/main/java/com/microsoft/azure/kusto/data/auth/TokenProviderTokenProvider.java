// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class TokenProviderTokenProvider extends TokenProviderBase {
    private final Callable<String> tokenProvider;

    TokenProviderTokenProvider(@NotNull Callable<String> tokenProvider, @NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
        this.tokenProvider = tokenProvider;
    }

    @Override
    public String acquireAccessToken() throws DataServiceException, DataClientException {
        try {
            return tokenProvider.call();
        } catch (Exception e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }
    }
}