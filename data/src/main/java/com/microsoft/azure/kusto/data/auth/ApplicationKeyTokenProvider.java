// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientSecret;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;

import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class ApplicationKeyTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientSecret clientSecret;

    ApplicationKeyTokenProvider(@NotNull String applicationClientId, @NotNull IClientSecret clientSecret, @NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(applicationClientId, clusterUrl, authorityId);
        this.clientSecret = clientSecret;
    }

    @Override
    protected void onCloudInit() throws DataClientException {
        try {
            clientApplication = ConfidentialClientApplication.builder(applicationClientId, clientSecret).authority(aadAuthorityUrl).build();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }
}