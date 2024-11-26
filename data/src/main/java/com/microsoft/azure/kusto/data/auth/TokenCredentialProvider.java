package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.CredentialBuilderBase;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;

public class TokenCredentialProvider extends AzureIdentityTokenProvider {
    private final TokenCredential credential;

    TokenCredentialProvider(@NotNull String clusterUrl, @NotNull TokenCredential credential) throws URISyntaxException {
        super(clusterUrl, null);
        this.credential = credential;
    }

    @Override
    protected CredentialBuilderBase<?> initBuilder() {
        return null;
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return credential;
    }
}
