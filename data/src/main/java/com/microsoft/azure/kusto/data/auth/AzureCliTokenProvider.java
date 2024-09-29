package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.AzureCliCredentialBuilder;
import com.azure.identity.CredentialBuilderBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class AzureCliTokenProvider extends AzureIdentityTokenProvider {
    public AzureCliTokenProvider(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
    }

    @Override
    protected CredentialBuilderBase<?> initBuilder() {
        return new AzureCliCredentialBuilder();
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return ((AzureCliCredentialBuilder) builder).build();
    }
}
