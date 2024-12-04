// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.CredentialBuilderBase;

import java.net.URISyntaxException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ApplicationKeyTokenProvider extends AzureIdentityTokenProvider {
    private final String clientSecret;

    public ApplicationKeyTokenProvider(@NotNull String clusterUrl, String clientId, String clientSecret, String tenantId,
            @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, tenantId, clientId, httpClient);
        this.clientSecret = clientSecret;
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return ((ClientSecretCredentialBuilder) builder)
                .clientSecret(clientSecret)
                .build();
    }

    @Override
    protected CredentialBuilderBase<?> initBuilder() {
        return new ClientSecretCredentialBuilder();
    }
}
