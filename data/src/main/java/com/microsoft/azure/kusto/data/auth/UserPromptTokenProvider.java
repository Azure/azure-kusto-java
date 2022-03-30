// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.InteractiveRequestParameters;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class UserPromptTokenProvider extends PublicAppTokenProviderBase {
    private static final int USER_PROMPT_TIMEOUT_MS = 120 * 1000;
    private static final URI redirectUri;

    static {
        URI tmp;
        try {
            tmp = new URI("http://localhost");
        } catch (URISyntaxException e) { // This cannot happen, but is necessary to allow for a static final variable whose instantiation can throw an exception
            tmp = null;
        }
        redirectUri = tmp;
    }

    private final String usernameHint;

    public UserPromptTokenProvider(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        this(clusterUrl, null, authorityId);
    }

    UserPromptTokenProvider(@NotNull String clusterUrl, String usernameHint, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
        this.usernameHint = usernameHint;
    }

    @Override
    protected IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            // This is the only auth method that allows the same application to be used for multiple distinct accounts, so reset account cache between sign-ins
            clientApplication = PublicClientApplication.builder(clientAppId).authority(aadAuthorityUrl).build();
            CompletableFuture<IAuthenticationResult> future =
                    clientApplication.acquireToken(InteractiveRequestParameters.builder(redirectUri).scopes(scopes).loginHint(usernameHint).build());
            result = future.get(USER_PROMPT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e, false);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e, false);
        }
        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireWithUserPrompt got 'null' authentication result",
                    false);
        }
        return result;
    }

    @Override
    IAccount getAccount(Set<IAccount> accountSet) {
        if (StringUtils.isNotBlank(usernameHint)) {
            return accountSet.stream().filter(u -> usernameHint.equalsIgnoreCase(u.username())).findAny().orElse(null);
        } else {
            if (accountSet.isEmpty()) {
                return null;
            } else {
                // Normally we would filter accounts by the user authenticating, but there's only 1 per AadAuthenticationHelper instance
                return accountSet.iterator().next();
            }
        }
    }
}
