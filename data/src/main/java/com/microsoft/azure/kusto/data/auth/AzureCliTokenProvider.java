package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import org.apache.http.client.HttpClient;
import com.azure.identity.AzureCliCredential;
import com.azure.identity.AzureCliCredentialBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class AzureCliTokenProvider extends CloudDependentTokenProviderBase {
    public static final String AZURE_CLI_TOKEN_PROVIDER = "AzureCliTokenProvider";
    private TokenRequestContext tokenRequestContext;
    private AzureCliCredential azureCliCredential;

    public AzureCliTokenProvider(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);

    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataServiceException, DataClientException {
        super.initializeWithCloudInfo(cloudInfo);
        AzureCliCredentialBuilder builder = new AzureCliCredentialBuilder();

        if (httpClient != null) {
            builder = builder.httpClient(new HttpClientWrapper(httpClient));
        }

        this.azureCliCredential = builder.build();
        tokenRequestContext = new TokenRequestContext().addScopes(scopes.toArray(new String[0]));
    }

    @Override
    protected String acquireAccessTokenImpl() throws DataServiceException {
        AccessToken accessToken = azureCliCredential.getToken(tokenRequestContext).block();
        if (accessToken == null) {
            throw new DataServiceException(clusterUrl, "Couldn't get token from Azure Identity", true);
        }
        return accessToken.getToken();
    }

    @Override
    protected String getAuthMethod() {
        return AZURE_CLI_TOKEN_PROVIDER;
    }
}
