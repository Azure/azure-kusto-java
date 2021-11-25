// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class CallbackTokenProvider extends TokenProviderBase {
    private final Callable<String> tokenProvider;

    CallbackTokenProvider(@NotNull String clusterUrl, @NotNull Callable<String> tokenProvider) throws URISyntaxException {
        super(clusterUrl);
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