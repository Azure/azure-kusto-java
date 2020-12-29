package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public abstract class MsalTokenProviderBase extends TokenProviderBase {
    protected static final String ORGANIZATION_URI_SUFFIX = "organizations";
    protected static final String ERROR_INVALID_AUTHORITY_URL = "Error acquiring ApplicationAccessToken due to invalid Authority URL";
    protected static final int TIMEOUT_MS = 20 * 1000;
    protected String aadAuthorityUrl;

    MsalTokenProviderBase(@NotNull String clusterUrl, String authorityId) throws URISyntaxException {
        super(clusterUrl);
        aadAuthorityUrl = determineAadAuthorityUrl(authorityId);
    }

    private String determineAadAuthorityUrl(String authorityId) {
        if (authorityId == null) {
            authorityId = ORGANIZATION_URI_SUFFIX;
        }

        String authorityUrl;
        String aadAuthorityFromEnv = System.getenv("AadAuthorityUri");
        if (aadAuthorityFromEnv == null) {
            authorityUrl = String.format("https://login.microsoftonline.com/%s/", authorityId);
        } else {
            authorityUrl = String.format("%s%s%s/", aadAuthorityFromEnv, aadAuthorityFromEnv.endsWith("/") ? "" : "/", authorityId);
        }
        return authorityUrl;
    }

    public String acquireAccessToken() throws DataServiceException, DataClientException {
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

    protected IAuthenticationResult acquireAccessTokenSilentlyMsal() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException, DataServiceException {
        throw new DataServiceException("Cannot obtain a token silently for Authentication type '" + this.getClass().getSimpleName() + "'");
    }

    protected IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException {
        throw new DataServiceException("Cannot obtain a new token for Authentication type '" + this.getClass().getSimpleName() + "'");
    }

    SilentParameters getSilentParameters(Set<IAccount> accountSet) {
        IAccount account = getAccount(accountSet);
        if (account != null) {
            return SilentParameters.builder(scopes).account(account).authorityUrl(aadAuthorityUrl).build();
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