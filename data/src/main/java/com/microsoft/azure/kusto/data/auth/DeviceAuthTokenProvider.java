package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.CredentialBuilderBase;
import com.azure.identity.DeviceCodeCredentialBuilder;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class DeviceAuthTokenProvider extends AzureIdentityTokenProvider {
    DeviceAuthTokenProvider(@NotNull String clusterUrl, @Nullable String tenantId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, null, tenantId, httpClient);
    }

    @Override
    protected CredentialBuilderBase<?> initBuilder() {
        return new DeviceCodeCredentialBuilder();
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return ((DeviceCodeCredentialBuilder) builder).build();
    }
}
