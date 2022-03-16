// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class PublicAppTokenProviderBase extends MsalTokenProviderBase {
    protected IPublicClientApplication clientApplication;
    protected String clientAppId;

    PublicAppTokenProviderBase(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataClientException, DataServiceException {
        super.initializeWithCloudInfo(cloudInfo);
        try {
            clientAppId = cloudInfo.getKustoClientAppId();
            clientApplication = PublicClientApplication.builder(clientAppId).authority(aadAuthorityUrl).build();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }

    @Override
    protected IAuthenticationResult acquireAccessTokenSilentlyMsal() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Set<IAccount>> accounts = clientApplication.getAccounts();
        return clientApplication.acquireTokenSilently(getSilentParameters(accounts.join())).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}
