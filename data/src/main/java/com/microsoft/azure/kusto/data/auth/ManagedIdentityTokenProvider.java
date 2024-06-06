package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.CredentialBuilderBase;
import com.azure.identity.ManagedIdentityCredentialBuilder;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class ManagedIdentityTokenProvider extends AzureIdentityTokenProvider {
    private final String managedIdentityClientId;

    public ManagedIdentityTokenProvider(@NotNull String clusterUrl, String managedIdentityClientId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.managedIdentityClientId = managedIdentityClientId;
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return ((ManagedIdentityCredentialBuilder) builder).build();
    }

    @Override
    @NotNull
    protected CredentialBuilderBase<?> initBuilder() {
        return new ManagedIdentityCredentialBuilder()
                .clientId("managedIdentityClientId");
    }
}
