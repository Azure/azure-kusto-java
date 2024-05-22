package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.HttpClient;
import com.azure.identity.implementation.IdentityClientOptions;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;

public abstract class AzureIdentityTokenProvider extends CloudDependentTokenProviderBase {
    private final String authMethod;
    private TokenCredential cred;
    private TokenRequestContext tokenRequestContext;

    AzureIdentityTokenProvider(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        authMethod = getClass().getSimpleName();
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return cred.getToken(tokenRequestContext).map(AccessToken::getToken);
    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataClientException, DataServiceException {
        super.initializeWithCloudInfo(cloudInfo);
        cred = createTokenCredential();
        tokenRequestContext = new TokenRequestContext().addScopes(scopes.toArray(new String[0]));
    }

    @Override
    protected String getAuthMethod() {
        return authMethod;
    }

    protected abstract TokenCredential createTokenCredential();
}
