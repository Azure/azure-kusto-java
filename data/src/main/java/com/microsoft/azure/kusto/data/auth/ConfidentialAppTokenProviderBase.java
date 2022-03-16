// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IConfidentialClientApplication;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

public abstract class ConfidentialAppTokenProviderBase extends MsalTokenProviderBase {
    final String applicationClientId;
    IConfidentialClientApplication clientApplication;

    ConfidentialAppTokenProviderBase(@NotNull String clusterUrl, @NotNull String applicationClientId, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
        this.applicationClientId = applicationClientId;
    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataClientException, DataServiceException {
        super.initializeWithCloudInfo(cloudInfo);
        try {
            clientApplication = getClientApplication();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }

    @Override
    protected IAuthenticationResult acquireNewAccessToken() throws DataServiceException {
        IAuthenticationResult result;
        try {
            CompletableFuture<IAuthenticationResult> future = clientApplication.acquireToken(
                    ClientCredentialParameters.builder(scopes).build());
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e, false);
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireNewAccessToken got 'null' authentication result", false);
        }
        return result;
    }

    @Override
    protected IAuthenticationResult acquireAccessTokenSilentlyMsal()
            throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Set<IAccount>> accounts = clientApplication.getAccounts();
        return clientApplication
                .acquireTokenSilently(getSilentParameters(accounts.join()))
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    protected abstract IConfidentialClientApplication getClientApplication() throws MalformedURLException;
}
