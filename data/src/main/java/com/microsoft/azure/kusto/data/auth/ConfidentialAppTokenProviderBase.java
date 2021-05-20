// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.*;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.TriState;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class ConfidentialAppTokenProviderBase extends MsalTokenProviderBase {
    IConfidentialClientApplication clientApplication;

    ConfidentialAppTokenProviderBase(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
    }

    @Override
    protected IAuthenticationResult acquireNewAccessToken() throws DataServiceException {
        IAuthenticationResult result;
        try {
            CompletableFuture<IAuthenticationResult> future = clientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e, TriState.FALSE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e, TriState.FALSE);
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireNewAccessToken got 'null' authentication result",
                    TriState.DONT_KNOW);
        }
        return result;
    }

    @Override
    protected IAuthenticationResult acquireAccessTokenSilentlyMsal() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Set<IAccount>> accounts = clientApplication.getAccounts();
        return clientApplication.acquireTokenSilently(getSilentParameters(accounts.join())).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }
}