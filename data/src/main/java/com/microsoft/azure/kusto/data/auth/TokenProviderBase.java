// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

public abstract class TokenProviderBase {
    protected static final String ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN = "Error acquiring ApplicationAccessToken";
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final String clusterUrl;

    TokenProviderBase(@NotNull String clusterUrl) throws URISyntaxException {
        this.clusterUrl = UriUtils.setPathForUri(clusterUrl, "");
    }

    public abstract String acquireAccessToken() throws DataServiceException, DataClientException;
}