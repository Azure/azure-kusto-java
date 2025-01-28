// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.aad.msal4j.IConfidentialClientApplication;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import com.azure.core.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// Azure identity doesn't provide a solution for all certificate types, so for now we still use MSAL for this.

public class ApplicationCertificateTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientCertificate clientCertificate;

    ApplicationCertificateTokenProvider(@NotNull String clusterUrl, @NotNull String applicationClientId, @NotNull IClientCertificate clientCertificate,
            String authorityId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, applicationClientId, authorityId, httpClient);
        this.clientCertificate = clientCertificate;
    }

    @Override
    protected IConfidentialClientApplication getClientApplication() throws MalformedURLException {
        ConfidentialClientApplication.Builder builder = ConfidentialClientApplication.builder(applicationClientId, clientCertificate)
                .authority(aadAuthorityUrl)
                .validateAuthority(false);
        if (httpClient != null) {
            builder.httpClient(new HttpClientWrapper(httpClient));
        }
        return clientApplication = builder
                .build();
    }
}
