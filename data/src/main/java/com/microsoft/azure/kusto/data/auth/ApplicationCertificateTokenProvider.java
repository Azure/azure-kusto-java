// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCertificate;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class ApplicationCertificateTokenProvider extends ConfidentialAppTokenProviderBase {
    ApplicationCertificateTokenProvider(@NotNull String applicationClientId, @NotNull IClientCertificate clientCertificate, @NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl, authorityId);
        try {
            clientApplication = ConfidentialClientApplication.builder(applicationClientId, clientCertificate).authority(aadAuthorityUrl).validateAuthority(false).build();
        } catch (MalformedURLException e) {
            throw new URISyntaxException(aadAuthorityUrl, ERROR_INVALID_AUTHORITY_URL);
        }
    }
}