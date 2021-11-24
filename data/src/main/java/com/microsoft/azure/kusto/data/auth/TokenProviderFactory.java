// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.aad.msal4j.IClientSecret;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class TokenProviderFactory {
    private TokenProviderFactory() {
        // Hide constructor, since this is a Factory
    }

    public static TokenProviderBase createTokenProvider(@NotNull ConnectionStringBuilder csb) throws URISyntaxException {
        String clusterUrl = csb.getClusterUrl();
        String authorityId = csb.getAuthorityId();
        if (StringUtils.isNotBlank(csb.getApplicationClientId())) {
            if (StringUtils.isNotBlank(csb.getApplicationKey())) {
                String clientId = csb.getApplicationClientId();
                IClientSecret clientSecret = ClientCredentialFactory.createFromSecret(csb.getApplicationKey());
                return new ApplicationKeyTokenProvider(clusterUrl, clientId, clientSecret, authorityId);
            } else if (csb.getX509CertificateChain() != null && !csb.getX509CertificateChain().isEmpty() && csb.getPrivateKey() != null) {
                IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificateChain(csb.getPrivateKey(), csb.getX509CertificateChain());
                String applicationClientId = csb.getApplicationClientId();
                return new SubjectNameIssuerTokenProvider(clusterUrl, applicationClientId, clientCertificate, authorityId);
            } else if (csb.getX509Certificate() != null && csb.getPrivateKey() != null) {
                IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificate(csb.getPrivateKey(), csb.getX509Certificate());
                String applicationClientId = csb.getApplicationClientId();
                return new ApplicationCertificateTokenProvider(clusterUrl, applicationClientId, clientCertificate, authorityId);
            } else {
                throw new IllegalArgumentException("No token provider exists for the provided ConnectionStringBuilder");
            }
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            String accessToken = csb.getAccessToken();
            return new AccessTokenTokenProvider(clusterUrl, accessToken);
        } else if (csb.getTokenProvider() != null) {
            Callable<String> tokenProvider = csb.getTokenProvider();
            return new CallbackTokenProvider(clusterUrl, tokenProvider);
        } else if(csb.isUseDeviceCodeAuth()) {
            return new DeviceAuthTokenProvider(clusterUrl, authorityId);
        } else if (csb.isUseManagedIdentityAuth()) {
            return new ManagedIdentityTokenProvider(clusterUrl, csb.getManagedIdentityClientId());
        } else if (csb.isUseUserPromptAuth()) {
            if (StringUtils.isNotBlank(csb.getUserUsernameHint())) {
                String usernameHint = csb.getUserUsernameHint();
                return new UserPromptTokenProvider(clusterUrl, usernameHint, authorityId);
            }
            return new UserPromptTokenProvider(clusterUrl, authorityId);
        } else {
            throw new IllegalArgumentException("No token provider exists for the provided ConnectionStringBuilder");
        }
    }
}