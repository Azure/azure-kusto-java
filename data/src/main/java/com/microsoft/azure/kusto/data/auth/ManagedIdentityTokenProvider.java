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
    private TokenRequestContext tokenRequestContext;

    public ManagedIdentityTokenProvider(String managedIdentityClientId, @NotNull String clusterUrl) throws URISyntaxException {
        super(clusterUrl);
        ManagedIdentityCredentialBuilder builder = new ManagedIdentityCredentialBuilder();
        if (StringUtils.isNotBlank(managedIdentityClientId)) {
            builder = builder.clientId(managedIdentityClientId); // only required for user assigned
        }
        this.managedIdentityCredential = builder.build();
        tokenRequestContext = new TokenRequestContext().addScopes(clusterUrl + "/.default");
    }

    private void setRequiredMembersBasedOnCloudInfo() throws DataServiceException {
        tokenRequestContext = new TokenRequestContext().addScopes(determineScope());
    }

    @Override
    public String acquireAccessToken() throws DataServiceException {
        initializeCloudInfo();
        setRequiredMembersBasedOnCloudInfo();
        AccessToken accessToken = managedIdentityCredential.getToken(tokenRequestContext).block();
        if (accessToken == null) {
            throw new DataServiceException(clusterUrl, "Couldn't get token from Azure Identity", true);
        }
        return accessToken.getToken();
    }
}