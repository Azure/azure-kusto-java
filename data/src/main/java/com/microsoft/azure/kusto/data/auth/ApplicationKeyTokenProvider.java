// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientSecret;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import org.jetbrains.annotations.NotNull;

public class ApplicationKeyTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientSecret clientSecret;

    ApplicationKeyTokenProvider(
            @NotNull String clusterUrl,
            @NotNull String applicationClientId,
            @NotNull IClientSecret clientSecret,
            String authorityId)
            throws URISyntaxException {
        super(clusterUrl, applicationClientId, authorityId);
        this.clientSecret = clientSecret;
    }

    @Override
    protected void setClientApplicationBasedOnCloudInfo() throws DataClientException {
        try {
            clientApplication = ConfidentialClientApplication.builder(applicationClientId, clientSecret)
                    .authority(aadAuthorityUrl)
                    .build();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }
}
