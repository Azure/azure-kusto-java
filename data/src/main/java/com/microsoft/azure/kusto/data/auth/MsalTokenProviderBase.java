package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class MsalTokenProviderBase extends TokenProviderBase {
    protected static final String ORGANIZATION_URI_SUFFIX = "organizations";
    protected static final String ERROR_INVALID_AUTHORITY_URL = "Error acquiring ApplicationAccessToken due to invalid Authority URL";
    protected static final int TIMEOUT_MS = 20 * 1000;
    private static final String PersonalTenantIdV2AAD = "9188040d-6c67-4c5b-b112-36a304b66dad"; // Identifies MSA accounts
    protected final Set<String> scopes = new HashSet<>();
    private final String authorityId;
    private boolean isCloudInfoInit = false;
    protected String aadAuthorityUrl;
    protected CloudInfo cloudInfo = null;


    MsalTokenProviderBase(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl);
        this.authorityId = authorityId;
    }

    private String determineAadAuthorityUrl(String authorityId) throws URISyntaxException {
        if (authorityId == null) {
            authorityId = ORGANIZATION_URI_SUFFIX;
        }

        String aadAuthorityFromEnv = System.getenv("AadAuthorityUri");
        return UriUtils.setPathForUri(aadAuthorityFromEnv == null ? cloudInfo.getLoginEndpoint(): aadAuthorityFromEnv, authorityId, true);
    }

    @Override
    public String acquireAccessToken() throws DataServiceException, DataClientException {
        initializeCloudInfo();
        IAuthenticationResult accessTokenResult = acquireAccessTokenSilently();
        if (accessTokenResult == null) {
            accessTokenResult = acquireNewAccessToken();
        }
        return accessTokenResult.accessToken();
    }

    protected void initializeCloudInfo() throws DataServiceException, DataClientException {
        if (isCloudInfoInit) {
            return;
        }
        synchronized (this) {
            if (isCloudInfoInit) {
                return;
            }

            cloudInfo = CloudInfo.retrieveCloudInfoForCluster(clusterUrl);
            try {
                aadAuthorityUrl = determineAadAuthorityUrl(authorityId);
            } catch (URISyntaxException e) {
                throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
            }

            String resourceUri = cloudInfo.getKustoServiceResourceId();
            if (cloudInfo.isLoginMfaRequired()) {
                resourceUri = resourceUri.replace(".kusto.", ".kustomfa.");
            }

            String scope;
            try {
                scope = UriUtils.setPathForUri(resourceUri, ".default");
            } catch (URISyntaxException e) {
                throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
            }
            scopes.add(scope);

            onCloudInit();

            isCloudInfoInit = true;
        }
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

    protected abstract void onCloudInit() throws DataClientException;

    protected abstract IAuthenticationResult acquireAccessTokenSilentlyMsal() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException, DataServiceException;

    protected abstract IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException;

    SilentParameters getSilentParameters(Set<IAccount> accountSet) {
        IAccount account = getAccount(accountSet);
        if (account != null) {
            String authorityUrl = aadAuthorityUrl;

            if (account.homeAccountId() != null && account.homeAccountId().endsWith(PersonalTenantIdV2AAD)) {
                authorityUrl = cloudInfo.getFirstPartyAuthorityUrl();
            }

            return SilentParameters.builder(scopes).account(account).authorityUrl(authorityUrl).build();
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