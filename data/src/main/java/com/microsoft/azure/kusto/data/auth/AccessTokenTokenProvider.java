// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;

public class AccessTokenTokenProvider extends TokenProviderBase {
    private final String accessToken;

    AccessTokenTokenProvider(@NotNull String clusterUrl, @NotNull String accessToken) throws URISyntaxException {
        super(clusterUrl);
        this.accessToken = accessToken;
    }

    @Override
    public String acquireAccessToken() throws DataServiceException, DataClientException {
        return accessToken;
    }
}