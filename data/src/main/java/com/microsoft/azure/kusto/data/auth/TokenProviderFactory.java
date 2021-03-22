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
        // Hide constructor as this is a Factory
    }

    public static TokenProviderBase createTokenProvider(@NotNull ConnectionStringBuilder csb) throws URISyntaxException {
        String clusterUrl = csb.getClusterUrl();
        String authorityId = csb.getAuthorityId();
        if (StringUtils.isNotBlank(csb.getApplicationClientId()) && StringUtils.isNotBlank(csb.getApplicationKey())) {
            String clientId = csb.getApplicationClientId();
            IClientSecret clientSecret = ClientCredentialFactory.createFromSecret(csb.getApplicationKey());
            return new ApplicationKeyTokenProvider(clientId, clientSecret, clusterUrl, authorityId);
        } else if (csb.getX509Certificate() != null && csb.getPrivateKey() != null && StringUtils.isNotBlank(csb.getApplicationClientId())) {
            IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificate(csb.getPrivateKey(), csb.getX509Certificate());
            String applicationClientId = csb.getApplicationClientId();
            return new ApplicationCertificateTokenProvider(applicationClientId, clientCertificate, clusterUrl, authorityId);
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            String accessToken = csb.getAccessToken();
            return new AccessTokenTokenProvider(accessToken, clusterUrl);
        } else if (csb.getTokenProvider() != null) {
            Callable<String> tokenProvider = csb.getTokenProvider();
            return new CallbackTokenProvider(tokenProvider, clusterUrl);
        } else if(csb.isUseDeviceCodeAuth()) {
            return new DeviceAuthTokenProvider(clusterUrl,authorityId);
        } else {
            if (StringUtils.isNotBlank(csb.getUserUsernameHint())) {
                String usernameHint = csb.getUserUsernameHint();
                return new UserPromptTokenProvider(usernameHint, clusterUrl, authorityId);
            }
            return new UserPromptTokenProvider(clusterUrl, authorityId);
        }
    }
}