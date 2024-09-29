// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.CredentialBuilderBase;
import com.azure.identity.InteractiveBrowserCredentialBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

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
