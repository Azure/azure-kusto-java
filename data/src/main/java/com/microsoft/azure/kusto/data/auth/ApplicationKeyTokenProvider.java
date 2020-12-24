// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientSecret;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class ApplicationKeyTokenProvider extends ConfidentialAppTokenProviderBase {
    ApplicationKeyTokenProvider(@NotNull String applicationClientId, @NotNull IClientSecret clientSecret, @NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
        try {
            clientApplication = ConfidentialClientApplication.builder(applicationClientId, clientSecret).authority(aadAuthorityUrl).build();
        } catch (MalformedURLException e) {
            throw new URISyntaxException(aadAuthorityUrl, ERROR_INVALID_AUTHORITY_URL);
        }
    }
}