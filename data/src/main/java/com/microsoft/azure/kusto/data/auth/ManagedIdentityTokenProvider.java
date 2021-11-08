package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;

public class ManagedIdentityTokenProvider extends TokenProviderBase {
    private final ManagedIdentityCredential managedIdentityCredential;
    private final TokenRequestContext tokenRequestContext;

    public ManagedIdentityTokenProvider(String managedIdentityClientId, @NotNull String clusterUrl) throws URISyntaxException {
        super(clusterUrl);
        if (StringUtils.isNotBlank(managedIdentityClientId)) {
            this.managedIdentityCredential = new ManagedIdentityCredentialBuilder()
                    .clientId(managedIdentityClientId) // only required for user assigned
                    .build();
        } else {
            this.managedIdentityCredential = new ManagedIdentityCredentialBuilder().build();
        }
        tokenRequestContext = new TokenRequestContext().addScopes(clusterUrl);
    }

    @Override
    public String acquireAccessToken() throws DataServiceException {
        AccessToken accessToken = managedIdentityCredential.getToken(tokenRequestContext).block();
        if (accessToken == null) {
            throw new DataServiceException(clusterUrl, "Couldn't get token from Azure Identity", true);
        }
        return accessToken.getToken();
    }
}