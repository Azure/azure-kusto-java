package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

public class SubjectNameIssuerTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientCertificate clientCertificate;

    SubjectNameIssuerTokenProvider(@NotNull String applicationClientId, @NotNull IClientCertificate clientCertificate, @NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(applicationClientId, clusterUrl, authorityId);
        this.clientCertificate = clientCertificate;
    }

    @Override
    protected void onCloudInit() throws DataClientException {
        try {
            clientApplication = ConfidentialClientApplication.builder(applicationClientId, clientCertificate).authority(aadAuthorityUrl).validateAuthority(false).sendX5c(true).build();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }
}