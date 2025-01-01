// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.IClientCertificate;
import org.apache.commons.lang3.StringUtils;
import com.azure.core.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;
import java.util.Collections;

public class TokenProviderFactory {
    private TokenProviderFactory() {
        // Hide constructor, since this is a Factory
    }

    public static TokenProviderBase createTokenProvider(@NotNull ConnectionStringBuilder csb, @Nullable HttpClient httpClient) throws URISyntaxException {
        String clusterUrl = csb.getClusterUrl();
        String authorityId = csb.getAuthorityId();
        Mono<String> asyncTokenProvider = csb.getAsyncTokenProvider();

        if (StringUtils.isNotBlank(csb.getApplicationClientId())) {
            if (StringUtils.isNotBlank(csb.getApplicationKey())) {
                return new ApplicationKeyTokenProvider(clusterUrl, csb.getApplicationClientId(), csb.getApplicationKey(), authorityId, httpClient);
            } else if (csb.getPrivateKey() != null && ((csb.getX509CertificateChain() != null && !csb.getX509CertificateChain().isEmpty()) || csb.getX509Certificate() != null)) {
                return new CertificateTokenProvider(clusterUrl,
                        csb.getAuthorityId(),
                        csb.getApplicationClientId(),
                        httpClient,
                        csb.getPrivateKey().getEncoded(),
                        csb.getX509CertificateChain() != null ? csb.getX509CertificateChain() : Collections.singletonList(csb.getX509Certificate()),
                        csb.getX509CertificateChain() != null
                );
            } else {
                throw new IllegalArgumentException("No token provider exists for the provided ConnectionStringBuilder");
            }
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            String accessToken = csb.getAccessToken();
            return new AccessTokenTokenProvider(clusterUrl, accessToken);
        } else if (csb.getTokenProvider() != null) {
            return new CallbackTokenProvider(clusterUrl, csb.getTokenProvider());
        } else if (asyncTokenProvider != null) {
            return new AsyncCallbackTokenProvider(clusterUrl, asyncTokenProvider);
        } else if (csb.getCustomTokenCredential() != null) {
          return new TokenCredentialProvider(clusterUrl, csb.getCustomTokenCredential());
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
            return null;
        }
    }
}
