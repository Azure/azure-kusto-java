package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.util.Context;
import com.azure.core.util.tracing.ProcessKind;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import com.microsoft.azure.kusto.data.instrumentation.DistributedTracing;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ManagedIdentityTokenProvider extends CloudDependentTokenProviderBase {
    public static final String MANAGED_IDENTITY_TOKEN_PROVIDER = "ManagedIdentityTokenProvider";
    private final String managedIdentityClientId;
    private ManagedIdentityCredential managedIdentityCredential;
    private TokenRequestContext tokenRequestContext;

    public ManagedIdentityTokenProvider(@NotNull String clusterUrl, String managedIdentityClientId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.managedIdentityClientId = managedIdentityClientId;
    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataServiceException, DataClientException {
        super.initializeWithCloudInfo(cloudInfo);
        ManagedIdentityCredentialBuilder builder = new ManagedIdentityCredentialBuilder();
        if (StringUtils.isNotBlank(managedIdentityClientId)) {
            builder = builder.clientId(managedIdentityClientId); // only required for user assigned
        }
        if (httpClient != null) {
            builder = builder.httpClient(new HttpClientWrapper(httpClient));
        }
        this.managedIdentityCredential = builder.build();
        tokenRequestContext = new TokenRequestContext().addScopes(scopes.toArray(new String[0]));
    }

    @Override
    public String acquireAccessTokenImpl() throws DataServiceException {
        // trace acquireNewAccessToken
        try (DistributedTracing.Span span = DistributedTracing.startSpan(getAuthMethod().concat(".acquireAccessTokenImpl"), Context.NONE, ProcessKind.PROCESS,
                null)) {
            try {
                AccessToken accessToken = managedIdentityCredential.getToken(tokenRequestContext).block();
                if (accessToken == null) {
                    throw new DataServiceException(clusterUrl, "Couldn't get token from Azure Identity", true);
                }
                return accessToken.getToken();
            } catch (DataServiceException e) {
                span.addException(e);
                throw e;
            }
        }
    }

    @Override
    protected String getAuthMethod() {
        return MANAGED_IDENTITY_TOKEN_PROVIDER;
    }
}
