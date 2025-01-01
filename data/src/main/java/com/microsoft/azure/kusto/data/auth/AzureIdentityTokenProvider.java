package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.HttpClient;
import com.azure.identity.AadCredentialBuilderBase;
import com.azure.identity.CredentialBuilderBase;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.URISyntaxException;

public abstract class AzureIdentityTokenProvider extends CloudDependentTokenProviderBase {
    private String clientId;
    private final String tenantId;
    TokenCredential cred;
    TokenRequestContext tokenRequestContext;

    protected static final String ORGANIZATION_URI_SUFFIX = "organizations";
    protected static final String ERROR_INVALID_AUTHORITY_URL = "Error acquiring ApplicationAccessToken due to invalid Authority URL";

    private String determineAadAuthorityUrl(CloudInfo cloudInfo) throws DataClientException {
        String aadAuthorityUrlFromEnv = System.getenv("AadAuthorityUri");
        String authorityIdToUse = tenantId != null ? tenantId : ORGANIZATION_URI_SUFFIX;
        try {
            return UriUtils.setPathForUri(aadAuthorityUrlFromEnv == null ? cloudInfo.getLoginEndpoint() : aadAuthorityUrlFromEnv, authorityIdToUse, true);
        } catch (URISyntaxException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }

    AzureIdentityTokenProvider(@NotNull String clusterUrl, @Nullable String tenantId, @Nullable String clientId,
            @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.tenantId = tenantId;
        this.clientId = clientId;
    }

    AzureIdentityTokenProvider(@NotNull String clusterUrl, @Nullable HttpClient httpClient) throws URISyntaxException {
        this(clusterUrl, null, null, httpClient);
    }

    @Override
    protected final Mono<String> acquireAccessTokenImpl() {
        return cred.getToken(tokenRequestContext).map(AccessToken::getToken);
    }

    @Override
    protected final void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataClientException, DataServiceException {
        super.initializeWithCloudInfo(cloudInfo);
        CredentialBuilderBase<?> builder = initBuilder();
        if (builder != null) {
            if (httpClient != null) {
                builder.httpClient(httpClient);
            }
            if (builder instanceof AadCredentialBuilderBase<?>) {
                AadCredentialBuilderBase<?> aadBuilder = (AadCredentialBuilderBase<?>) builder;
                if (tenantId != null)
                    aadBuilder.tenantId(tenantId);

                aadBuilder.authorityHost(determineAadAuthorityUrl(cloudInfo));

                if (clientId == null) {
                    clientId = cloudInfo.getKustoClientAppId();
                }

                aadBuilder.clientId(clientId);

            }
        }

        cred = createTokenCredential(builder);
        tokenRequestContext = new TokenRequestContext().addScopes(scopes.toArray(new String[0]));
    }

    public String getTenantId() {
        return tenantId;
    }

    protected abstract CredentialBuilderBase<?> initBuilder();

    // This method exists since there is no common build() method for all of the Azure Identity builders
    protected abstract TokenCredential createTokenCredential(CredentialBuilderBase<?> builder);
}
