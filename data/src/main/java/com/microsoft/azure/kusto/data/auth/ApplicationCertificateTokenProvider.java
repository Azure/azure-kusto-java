// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import org.jetbrains.annotations.NotNull;

public class ApplicationCertificateTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientCertificate clientCertificate;

    ApplicationCertificateTokenProvider(
            @NotNull String clusterUrl,
            @NotNull String applicationClientId,
            @NotNull IClientCertificate clientCertificate,
            String authorityId)
            throws URISyntaxException {
        super(clusterUrl, applicationClientId, authorityId);
        this.clientCertificate = clientCertificate;
    }

    @Override
    protected void setClientApplicationBasedOnCloudInfo() throws DataClientException {
        try {
            clientApplication = ConfidentialClientApplication.builder(applicationClientId, clientCertificate)
                    .authority(aadAuthorityUrl)
                    .validateAuthority(false)
                    .build();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }
}
