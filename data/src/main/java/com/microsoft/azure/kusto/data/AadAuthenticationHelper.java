// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.aad.msal4j.*;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class AadAuthenticationHelper {
    protected static final String DEFAULT_AAD_TENANT = "microsoft.com";
    protected static final String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    protected static final long MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS = 60000;
    protected static final String ERROR_INVALID_AUTHORITY_URL = "Error acquiring ApplicationAccessToken due to invalid Authority URL";
    protected static final String ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN = "Error acquiring ApplicationAccessToken";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final int TIMEOUT_MS = 60 * 1000;
    private final Set<String> scopes;
    private final String scope;
    private final AuthenticationType authenticationType;
    private final String aadAuthorityUrl;
    private String clientId;
    private IClientSecret clientSecret;
    private IClientCertificate clientCertificate;
    private String applicationClientId;
    private String userUsername;
    private URI redirectUri;
    private String accessToken;
    private Callable<String> tokenProvider;
    private final Lock lastClientApplicationLock = new ReentrantLock();
    private IAuthenticationResult lastAuthenticationResult;
    private IPublicClientApplication lastPublicClientApplication;
    private IConfidentialClientApplication lastConfidentialClientApplication;

    private enum AuthenticationType {
        AAD_USER_PROMPT(ClientApplicationType.PUBLIC),
        AAD_APPLICATION_KEY(ClientApplicationType.CONFIDENTIAL),
        AAD_APPLICATION_CERTIFICATE(ClientApplicationType.CONFIDENTIAL),
        AAD_ACCESS_TOKEN(ClientApplicationType.NOT_APPLICABLE),
        AAD_ACCESS_TOKEN_PROVIDER(ClientApplicationType.NOT_APPLICABLE);
        // TODO: Confirming that we don't need to support AAD Application Certificate Thumbprint nor AAD Application Certificate Subject And Issuer

        private final ClientApplicationType clientApplicationType;

        AuthenticationType(ClientApplicationType clientApplicationType) {
            this.clientApplicationType = clientApplicationType;
        }

        public ClientApplicationType getClientApplicationType() {
            return this.clientApplicationType;
        }
    }

    private enum ClientApplicationType {
        PUBLIC,
        CONFIDENTIAL,
        NOT_APPLICABLE
    }

    AadAuthenticationHelper(@NotNull ConnectionStringBuilder csb) throws URISyntaxException, DataClientException {
        URI clusterUrl = new URI(csb.getClusterUrl());
        scope = String.format("%s://%s/%s", clusterUrl.getScheme(), clusterUrl.getHost(), ".default");
        scopes = new HashSet<>();
        scopes.add(scope);

        if (StringUtils.isNotBlank(csb.getApplicationClientId()) && StringUtils.isNotBlank(csb.getApplicationKey())) {
            clientId = csb.getApplicationClientId();
            clientSecret = ClientCredentialFactory.createFromSecret(csb.getApplicationKey());
            authenticationType = AuthenticationType.AAD_APPLICATION_KEY;
        } else if (StringUtils.isNotBlank(csb.getUserUsername())) {
            userUsername = csb.getUserUsername(); // TODO: Should this be required? It's only used for a hint, so perhaps it should be optional, in which case AAD_USER_PROMPT is the else in this flow.
            redirectUri = new URI("http://localhost"); // TODO: Right?
            authenticationType = AuthenticationType.AAD_USER_PROMPT;
        } else if (csb.getX509Certificate() != null && csb.getPrivateKey() != null && StringUtils.isNotBlank(csb.getApplicationClientId())) {
            clientCertificate = ClientCredentialFactory.createFromCertificate(csb.getPrivateKey(), csb.getX509Certificate());
            applicationClientId = csb.getApplicationClientId();
            authenticationType = AuthenticationType.AAD_APPLICATION_CERTIFICATE;
        } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
            accessToken = csb.getAccessToken();
            authenticationType = AuthenticationType.AAD_ACCESS_TOKEN;
        } else if (csb.getTokenProvider() != null) {
            tokenProvider = csb.getTokenProvider();
            authenticationType = AuthenticationType.AAD_ACCESS_TOKEN_PROVIDER;
        } else {
            throw new DataClientException("No valid authentication type relevant to provided ConnectionStringBuilder parameters");
        }

        aadAuthorityUrl = determineAadAuthorityUrl(csb.getAuthorityId());
    }

    private String determineAadAuthorityUrl(String aadAuthorityUrl) {
        String aadAuthorityId = (aadAuthorityUrl == null ? DEFAULT_AAD_TENANT : aadAuthorityUrl);
        String aadAuthorityFromEnv = System.getenv("AadAuthorityUri");
        // TODO: Also support organization flow? (https://login.microsoftonline.com/organizations/)
        if (aadAuthorityFromEnv == null) {
            aadAuthorityUrl = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
        } else {
            aadAuthorityUrl = String.format("%s%s%s", aadAuthorityFromEnv, aadAuthorityFromEnv.endsWith("/") ? "" : "/", aadAuthorityId);
        }
        return aadAuthorityUrl;
    }

    String acquireAccessToken() throws DataServiceException, DataClientException {
        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN) {
            return accessToken;
        }

        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN_PROVIDER) {
            try {
                // TODO: Don't hold onto the lastAuthenticationResult as other methods do (have to make a new call every time), right?
                Future<String> future = executor.submit(tokenProvider);
                return getAsyncResultNonblocking(future);
            } catch (Exception e) {
                throw new DataClientException(scope, e.getMessage(), e);
            }
        }

        try {
            lastClientApplicationLock.lock();
            if (lastAuthenticationResult == null) {
                lastAuthenticationResult = acquireNewAccessToken();
            } else if (isInvalidToken()) {
                // TODO: I prefer to reset it than hold an invalid token. However, this prevents us from accessing the account/environment, which are likely still valid.
                lastAuthenticationResult = null;
                try {
                    lastAuthenticationResult = acquireAccessTokenByRefreshToken();
                } catch (Exception ex) {
                    // TODO: Catch so we can next acquire a new access token, right?
                    logger.error("Failed to acquire access token silently (via cache or refresh token). Attempting to get new access token.", ex);
                }

                if (lastAuthenticationResult == null) {
                    lastAuthenticationResult = acquireNewAccessToken();
                }
            }
            if (lastAuthenticationResult == null) {
                throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN);
            }
            return lastAuthenticationResult.accessToken();
        } finally {
            lastClientApplicationLock.unlock();
        }
    }

    private IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException {
        switch (authenticationType) {
            case AAD_APPLICATION_KEY:
                return acquireWithAadApplicationKey();
            case AAD_APPLICATION_CERTIFICATE:
                return acquireWithAadApplicationClientCertificate();
            case AAD_USER_PROMPT:
                return acquireWithUserPrompt();
            default:
                throw new DataClientException("Authentication type '" + authenticationType.name() + "' is invalid");
        }
    }

    private boolean isInvalidToken() {
        return lastAuthenticationResult == null || lastAuthenticationResult.expiresOnDate().before(dateInAMinute());
    }

    private IAuthenticationResult acquireWithAadApplicationKey() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            lastConfidentialClientApplication = ConfidentialClientApplication.builder(clientId, clientSecret).authority(aadAuthorityUrl).build();
            CompletableFuture<IAuthenticationResult> future = lastConfidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            /*
             * TODO: These get() calls are blocking. I would prefer to change the entire API and return the CompletableFuture so that this will be an async method,
             *  though I doubt others would find it worthwhile. Else, I can unblock the thread with sleep(exponentialBackoff) as with AAD_ACCESS_TOKEN_PROVIDER,
             *  but I doubt others would find that worthwhile either.
             */
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(scope, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        }

        if (result == null) {
            throw new DataServiceException(scope, "acquireWithAadApplicationKey got 'null' authentication result");
        }
        return result;
    }

    IAuthenticationResult acquireWithAadApplicationClientCertificate() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            lastConfidentialClientApplication = ConfidentialClientApplication.builder(applicationClientId, clientCertificate).authority(aadAuthorityUrl).validateAuthority(false).build();
            CompletableFuture<IAuthenticationResult> future = lastConfidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(scope, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        }

        if (result == null) {
            throw new DataServiceException(scope, "acquireWithAadApplicationClientCertificate got 'null' authentication result");
        }
        return result;
    }

    private IAuthenticationResult acquireWithUserPrompt() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            /*
             *  TODO: lastPublicClientApplication is always null here.
             *      But we can change that logic if we want the Authority hostname from last time (lastAuthenticationResult.environment()), which can be used for .domainHint()
             *      We also might want the username (lastAuthenticationResult.account()) from the last connection, because it's only a hint. So if they changed it, we don't want to keep suggesting the original.
             */
            lastPublicClientApplication = PublicClientApplication.builder(CLIENT_ID).authority(aadAuthorityUrl).build();
            CompletableFuture<IAuthenticationResult> future = lastPublicClientApplication.acquireToken(InteractiveRequestParameters.builder(redirectUri).scopes(scopes).loginHint(userUsername).build());
            result = future.get(120, TimeUnit.SECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(scope, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(scope, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        }
        if (result == null) {
            throw new DataServiceException(scope, "acquireWithUserPrompt got 'null' authentication result");
        }
        return result;
    }

    IAuthenticationResult acquireAccessTokenByRefreshToken() throws MalformedURLException, ExecutionException, InterruptedException, TimeoutException, DataClientException {
        CompletableFuture<Set<IAccount>> accounts;
        switch (authenticationType.getClientApplicationType()) {
            case CONFIDENTIAL:
                accounts = lastConfidentialClientApplication.getAccounts();
                return lastConfidentialClientApplication.acquireTokenSilently(getSilentParameters(accounts)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            case PUBLIC:
                accounts = lastPublicClientApplication.getAccounts();
                return lastPublicClientApplication.acquireTokenSilently(getSilentParameters(accounts)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            case NOT_APPLICABLE:
                throw new DataClientException("Cannot obtain a refresh token for Authentication type '" + authenticationType.name());
            default:
                throw new DataClientException("Authentication type '" + authenticationType.name() + "' is invalid");
        }
    }

    private SilentParameters getSilentParameters(CompletableFuture<Set<IAccount>> accounts) {
        IAccount account;
        if (StringUtils.isNotBlank(userUsername)) {
            account = accounts.join().stream().filter(u -> userUsername.equalsIgnoreCase(u.username())).findAny().orElse(null);
        } else {
            // Normally we would filter accounts by the user authenticating, but there's only 1 per AadAuthenticationHelper instance
            account = accounts.join().iterator().next();
        }

        // TODO: Is this useful?
        if (account == null) {
            account = lastAuthenticationResult.account();
        }

        return SilentParameters.builder(scopes).account(account).authorityUrl(aadAuthorityUrl).build();
    }

    // TODO: I see a valuable performance improvement by freeing this thread from blocking to do other things (perhaps the user is concurrently ingesting?). I suspect others will consider this overkill.
    private <T> T getAsyncResultNonblocking(Future<T> future) throws InterruptedException, ExecutionException, TimeoutException {
        int localTimeoutMs = TIMEOUT_MS;
        int waitPeriodMs = 50;
        while (!future.isDone() && localTimeoutMs > 0) {
            Thread.sleep(waitPeriodMs);
            localTimeoutMs -= waitPeriodMs;
            waitPeriodMs *= 2; // Exponential backoff
        }
        return future.get(1, TimeUnit.MICROSECONDS); // Timeout by this point
    }

    Date dateInAMinute() {
        return new Date(System.currentTimeMillis() + MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS);
    }
}