// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.BrowserCustomizationOptions;
import com.azure.identity.CredentialBuilderBase;
import com.azure.identity.InteractiveBrowserCredentialBuilder;
import com.azure.identity.UsernamePasswordCredentialBuilder;
import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.InteractiveRequestParameters;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class UserPromptTokenProvider extends AzureIdentityTokenProvider {
    private final String usernameHint;

    UserPromptTokenProvider(@NotNull String clusterUrl, @Nullable String usernameHint, String authorityId,
            @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, authorityId, null, httpClient);
        this.usernameHint = usernameHint;
    }


    @Override
    protected CredentialBuilderBase<?> initBuilder() {
        return new InteractiveBrowserCredentialBuilder()
                .loginHint(usernameHint);
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return ((InteractiveBrowserCredentialBuilder) builder).build();
    }
}
