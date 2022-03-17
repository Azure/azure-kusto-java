package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.aad.msal4j.IConfidentialClientApplication;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import org.jetbrains.annotations.NotNull;

public class SubjectNameIssuerTokenProvider extends ConfidentialAppTokenProviderBase {
    private final IClientCertificate clientCertificate;

    SubjectNameIssuerTokenProvider(
            @NotNull String clusterUrl,
            @NotNull String applicationClientId,
            @NotNull IClientCertificate clientCertificate,
            String authorityId) throws URISyntaxException {
        super(clusterUrl, applicationClientId, authorityId);
        this.clientCertificate = clientCertificate;
    }

    @Override
    protected IConfidentialClientApplication getClientApplication() throws MalformedURLException {
        return ConfidentialClientApplication.builder(applicationClientId, clientCertificate)
                .authority(aadAuthorityUrl)
                .validateAuthority(false)
                .sendX5c(true)
                .build();
    }
}
