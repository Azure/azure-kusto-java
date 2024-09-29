package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import org.apache.commons.lang3.StringUtils;
import com.azure.core.http.HttpClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class MsalTokenProviderBase extends CloudDependentTokenProviderBase {
    protected static final String ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN = "Error acquiring ApplicationAccessToken";
    protected static final String ORGANIZATION_URI_SUFFIX = "organizations";
    protected static final String ERROR_INVALID_AUTHORITY_URL = "Error acquiring ApplicationAccessToken due to invalid Authority URL";
    protected static final int TIMEOUT_MS = 20 * 1000;
    private static final String PERSONAL_TENANT_IDV2_AAD = "9188040d-6c67-4c5b-b112-36a304b66dad"; // Identifies MSA accounts
    private final String authorityId;
    protected String aadAuthorityUrl;
    private String firstPartyAuthorityUrl;

    MsalTokenProviderBase(@NotNull String clusterUrl, String authorityId, @Nullable HttpClient httpClient) throws URISyntaxException {
        super(clusterUrl, httpClient);
        this.authorityId = authorityId;
    }

    @Override
    protected void initializeWithCloudInfo(CloudInfo cloudInfo) throws DataClientException, DataServiceException {
        super.initializeWithCloudInfo(cloudInfo);
        aadAuthorityUrl = determineAadAuthorityUrl(cloudInfo);
        firstPartyAuthorityUrl = cloudInfo.getFirstPartyAuthorityUrl();
        // Some apis (e.g. device authentication) require the url to always end in backslash.
        firstPartyAuthorityUrl = StringUtils.appendIfMissing(firstPartyAuthorityUrl, "/");
    }

    private String determineAadAuthorityUrl(CloudInfo cloudInfo) throws DataClientException {
        String aadAuthorityUrlFromEnv = System.getenv("AadAuthorityUri");
        String authorityIdToUse = authorityId != null ? authorityId : ORGANIZATION_URI_SUFFIX;
        try {
            return UriUtils.setPathForUri(aadAuthorityUrlFromEnv == null ? cloudInfo.getLoginEndpoint() : aadAuthorityUrlFromEnv, authorityIdToUse, true);
        } catch (URISyntaxException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }

    @Override
    protected Mono<String> acquireAccessTokenImpl() {
        return Mono.fromCallable(this::acquireAccessTokenSilently)
                .switchIfEmpty(Mono.defer(() -> MonitoredActivity.wrap(Mono.fromCallable(this::acquireNewAccessToken),
                        getAuthMethod().concat(".acquireNewAccessToken"), getTracingAttributes())))
                .map(IAuthenticationResult::accessToken);
    }

    protected IAuthenticationResult acquireAccessTokenSilently() throws DataServiceException, DataClientException {
        try {
            return acquireAccessTokenSilentlyMsal();
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (TimeoutException | ExecutionException e) {
            return null; // Legitimate outcome, in which case a new token will be acquired
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null; // Legitimate outcome, in which case a new token will be acquired
        }
    }

    protected abstract IAuthenticationResult acquireAccessTokenSilentlyMsal()
            throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException, DataServiceException;

    protected abstract IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException;

    SilentParameters getSilentParameters(Set<IAccount> accountSet) {
        IAccount account = getAccount(accountSet);
        if (account != null) {
            String authorityUrl = aadAuthorityUrl;

            if (account.homeAccountId() != null && account.homeAccountId().endsWith(PERSONAL_TENANT_IDV2_AAD)) {
                authorityUrl = firstPartyAuthorityUrl;
            }

            return SilentParameters.builder(scopes, account).authorityUrl(authorityUrl).build();
        }
        return SilentParameters.builder(scopes).authorityUrl(aadAuthorityUrl).build();
    }

    IAccount getAccount(Set<IAccount> accountSet) {
        if (accountSet.isEmpty()) {
            return null;
        } else {
            // Normally we would filter accounts by the user authenticating, but there's only 1 per AadAuthenticationHelper instance
            return accountSet.iterator().next();
        }
    }
}
