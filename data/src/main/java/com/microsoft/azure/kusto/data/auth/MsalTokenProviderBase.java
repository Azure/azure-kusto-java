// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.azure.kusto.data.UriUtils;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

public abstract class MsalTokenProviderBase extends TokenProviderBase {
    protected static final String ORGANIZATION_URI_SUFFIX = "organizations";
    protected static final String ERROR_INVALID_AUTHORITY_URL =
            "Error acquiring ApplicationAccessToken due to invalid Authority URL";
    protected static final int TIMEOUT_MS = 20 * 1000;
    private static final String PERSONAL_TENANT_IDV2_AAD =
            "9188040d-6c67-4c5b-b112-36a304b66dad"; // Identifies MSA accounts
    protected final Set<String> scopes = new HashSet<>();
    private final String authorityId;
    protected String aadAuthorityUrl;

    MsalTokenProviderBase(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl);
        this.authorityId = authorityId;
    }

    protected void setRequiredMembersBasedOnCloudInfo() throws DataClientException, DataServiceException {
        aadAuthorityUrl = determineAadAuthorityUrl();
        scopes.add(determineScope());
        setClientApplicationBasedOnCloudInfo();
    }

    private String determineAadAuthorityUrl() throws DataClientException {
        String aadAuthorityUrlFromEnv = System.getenv("AadAuthorityUri");
        String authorityIdToUse = authorityId != null ? authorityId : ORGANIZATION_URI_SUFFIX;
        try {
            return UriUtils.setPathForUri(
                    aadAuthorityUrlFromEnv == null ? cloudInfo.getLoginEndpoint() : aadAuthorityUrlFromEnv,
                    authorityIdToUse,
                    true);
        } catch (URISyntaxException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        }
    }

    protected abstract void setClientApplicationBasedOnCloudInfo() throws DataClientException;

    @Override
    public String acquireAccessToken() throws DataServiceException, DataClientException {
        initializeCloudInfo();
        setRequiredMembersBasedOnCloudInfo();
        IAuthenticationResult accessTokenResult = acquireAccessTokenSilently();
        if (accessTokenResult == null) {
            accessTokenResult = acquireNewAccessToken();
        }
        return accessTokenResult.accessToken();
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
            throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException,
                    DataServiceException;

    protected abstract IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException;

    SilentParameters getSilentParameters(Set<IAccount> accountSet) {
        IAccount account = getAccount(accountSet);
        if (account != null) {
            String authorityUrl = aadAuthorityUrl;

            if (account.homeAccountId() != null && account.homeAccountId().endsWith(PERSONAL_TENANT_IDV2_AAD)) {
                authorityUrl = cloudInfo.getFirstPartyAuthorityUrl();
            }

            return SilentParameters.builder(scopes)
                    .account(account)
                    .authorityUrl(authorityUrl)
                    .build();
        }
        return SilentParameters.builder(scopes).authorityUrl(aadAuthorityUrl).build();
    }

    IAccount getAccount(Set<IAccount> accountSet) {
        if (accountSet.isEmpty()) {
            return null;
        } else {
            // Normally we would filter accounts by the user authenticating, but there's only 1 per
            // AadAuthenticationHelper instance
            return accountSet.iterator().next();
        }
    }
}
