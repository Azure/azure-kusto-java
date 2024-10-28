package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.HttpClient;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class AzureIdentityTokenProvider extends CloudDependentTokenProviderBase {
    public static final String AZURE_ID_TOKEN_PROVIDER = "AzureIdentityTokenProvider";
    private TokenRequestContext tokenRequestContext;
    private final TokenCredential tokenCredential;

    public AzureIdentityTokenProvider(@NotNull String clusterUrl, @Nullable HttpClient httpClient, @NotNull TokenCredential credentials) throws URISyntaxException {
        super(clusterUrl, httpClient);
        tokenCredential = credentials;
    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataServiceException, DataClientException {
        super.initializeWithCloudInfo(cloudInfo);
        tokenRequestContext = new TokenRequestContext().addScopes(scopes.toArray(new String[0]));
    }

    @Override
    protected String acquireAccessTokenImpl() throws DataServiceException {
        // Fixme: Make this async
        AccessToken accessToken = tokenCredential.getToken(tokenRequestContext).block();
        if (accessToken == null) {
            throw new DataServiceException(clusterUrl, "Couldn't get token from Azure Identity", true);
        }
        return accessToken.getToken();
    }

    @Override
    protected String getAuthMethod() {
        return AZURE_ID_TOKEN_PROVIDER;
    }
}
