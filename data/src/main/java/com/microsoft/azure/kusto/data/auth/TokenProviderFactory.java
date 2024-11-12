// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.IClientCertificate;
import org.apache.commons.lang3.StringUtils;
import com.azure.core.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class TokenProviderFactory {
    private TokenProviderFactory() {
        // Hide constructor, since this is a Factory
    }

    public static TokenProviderBase createTokenProvider(@NotNull ConnectionStringBuilder csb, @Nullable HttpClient httpClient) throws URISyntaxException {
        String clusterUrl = csb.getClusterUrl();
        String authorityId = csb.getAuthorityId();
        if (StringUtils.isNotBlank(csb.getApplicationClientId())) {
            if (StringUtils.isNotBlank(csb.getApplicationKey())) {
                return new ApplicationKeyTokenProvider(clusterUrl, csb.getApplicationClientId(), csb.getApplicationKey(), authorityId, httpClient);
            } else if (csb.getX509CertificateChain() != null && !csb.getX509CertificateChain().isEmpty() && csb.getPrivateKey() != null) {
                IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificateChain(csb.getPrivateKey(), csb.getX509CertificateChain());
                String applicationClientId = csb.getApplicationClientId();
                return new SubjectNameIssuerTokenProvider(clusterUrl, applicationClientId, clientCertificate, authorityId, httpClient);
            } else if (csb.getX509Certificate() != null && csb.getPrivateKey() != null) {
                IClientCertificate clientCertificate = ClientCredentialFactory.createFromCertificate(csb.getPrivateKey(), csb.getX509Certificate());
                String applicationClientId = csb.getApplicationClientId();
                return new ApplicationCertificateTokenProvider(clusterUrl, applicationClientId, clientCertificate, authorityId, httpClient);
            } else {
                throw new IllegalArgumentException("No token provider exists for the provided ConnectionStringBuilder");
            }
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            String accessToken = csb.getAccessToken();
            return new AccessTokenTokenProvider(clusterUrl, accessToken);
        } else if (csb.getTokenProvider() != null) {
            Callable<String> tokenProvider = csb.getTokenProvider();
            return new CallbackTokenProvider(clusterUrl, tokenProvider);
        } else if (csb.getAsyncTokenProvider() != null) {
            return new AsyncCallbackTokenProvider(clusterUrl, csb.getAsyncTokenProvider());
        } else if (csb.getCustomTokenCredential() != null) {
          return new CustomTokenCredentialProvider(clusterUrl, csb.getCustomTokenCredential(), csb.getCustomTokenRequestContext());
        } else if (csb.isUseDeviceCodeAuth()) {
            return new DeviceAuthTokenProvider(clusterUrl, authorityId, httpClient);
        } else if (csb.isUseManagedIdentityAuth()) {
            return new ManagedIdentityTokenProvider(clusterUrl, csb.getManagedIdentityClientId(), httpClient);
        } else if (csb.isUseAzureCli()) {
            return new AzureCliTokenProvider(clusterUrl, httpClient);
        } else if (csb.isUseUserPromptAuth()) {
            if (StringUtils.isNotBlank(csb.getUserUsernameHint())) {
                String usernameHint = csb.getUserUsernameHint();
                return new UserPromptTokenProvider(clusterUrl, usernameHint, authorityId, httpClient);
            }
            return new UserPromptTokenProvider(clusterUrl, null, authorityId, httpClient);
        } else {
            throw new IllegalArgumentException("No token provider exists for the provided ConnectionStringBuilder");
        }
    }
}
