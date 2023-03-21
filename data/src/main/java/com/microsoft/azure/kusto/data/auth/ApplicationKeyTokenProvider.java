// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientSecret;
import com.microsoft.aad.msal4j.IConfidentialClientApplication;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ApplicationKeyTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientSecret clientSecret;
    public static final String APPLICATION_KEY_TOKEN_PROVIDER = "ApplicationKeyTokenProvider";

    ApplicationKeyTokenProvider(@NotNull String clusterUrl, @NotNull String applicationClientId, @NotNull IClientSecret clientSecret,
            String authorityId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, applicationClientId, authorityId, httpClient);
        this.clientSecret = clientSecret;
    }

    @Override
    protected IConfidentialClientApplication getClientApplication() throws MalformedURLException {
        ConfidentialClientApplication.Builder authority = ConfidentialClientApplication.builder(applicationClientId, clientSecret)
                .authority(aadAuthorityUrl);
        if (httpClient != null) {
            authority.httpClient(new HttpClientWrapper(httpClient));
        }
        return authority.build();
    }

    @Override
    protected String getAuthMethodForTracing() {
        return APPLICATION_KEY_TOKEN_PROVIDER;
    }
}
