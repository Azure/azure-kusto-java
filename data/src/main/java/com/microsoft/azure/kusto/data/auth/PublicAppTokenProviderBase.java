// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.*;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class PublicAppTokenProviderBase extends MsalTokenProviderBase {
    // TODO: Get ClientId from CM endpoint
    static final String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    IPublicClientApplication clientApplication;

    PublicAppTokenProviderBase(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
    }

    @Override
    protected IAuthenticationResult acquireAccessTokenSilentlyMsal() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Set<IAccount>> accounts = clientApplication.getAccounts();
        return clientApplication.acquireTokenSilently(getSilentParameters(accounts.join())).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}