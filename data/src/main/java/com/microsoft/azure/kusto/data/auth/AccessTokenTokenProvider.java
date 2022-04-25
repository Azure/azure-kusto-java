// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import java.net.URISyntaxException;

import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AccessTokenTokenProvider extends TokenProviderBase {
    private final String accessToken;

    AccessTokenTokenProvider(@NotNull String clusterUrl, @NotNull String accessToken) throws URISyntaxException {
        super(clusterUrl);
        this.accessToken = accessToken;
    }

    @Override
    public String acquireAccessToken(@Nullable HttpClient httpClient) {
        return accessToken;
    }
}
