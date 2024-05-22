// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.microsoft.aad.msal4j.IClientCertificate;
import java.net.URISyntaxException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ApplicationCertificateTokenProvider extends AzureIdentityTokenProvider {
    private final String applicationClientId;
    private final IClientCertificate clientCertificate;
    private final String authorityId;

    ApplicationCertificateTokenProvider(@NotNull String clusterUrl, @NotNull String applicationClientId, @NotNull IClientCertificate clientCertificate,
            String authorityId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.applicationClientId = applicationClientId;
        this.clientCertificate = clientCertificate;
        this.authorityId = authorityId;
    }

    @Override
    protected TokenCredential createTokenCredential() {
        ClientCertificateCredentialBuilder builder = new ClientCertificateCredentialBuilder()
                .clientId(applicationClientId)
                .clientCertificatePassword()
        if (httpClient != null) {
            builder.httpClient(new HttpClientWrapper(httpClient));
        }
        return builder.build();

    }

}
