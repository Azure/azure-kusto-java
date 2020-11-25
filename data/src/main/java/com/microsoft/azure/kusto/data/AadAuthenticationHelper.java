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

class AadAuthenticationHelper {
    // TODO: Ran suggested using "organizations" instead of "microsoft.com" (as the DEFAULT_AAD_TENANT), but then acquireWithAadApplicationClientCertificate didn't work
    protected static final String ORGANIZATION_URI_SUFFIX = "microsoft.com";
    // TODO: Get ClientId from CM endpoint
    protected static final String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    protected static final long MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS = 60 * 1000;
    protected static final String ERROR_INVALID_AUTHORITY_URL = "Error acquiring ApplicationAccessToken due to invalid Authority URL";
    protected static final String ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN = "Error acquiring ApplicationAccessToken";
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final int TIMEOUT_MS = 20 * 1000;
    private static final int USER_PROMPT_TIMEOUT_MS = 120 * 1000;
    private final String clusterUrl;
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
    private IAuthenticationResult lastAuthenticationResult;
    private IPublicClientApplication publicClientApplication;
    private IConfidentialClientApplication confidentialClientApplication;

    private enum AuthenticationType {
        AAD_USER_PROMPT(ClientApplicationType.PUBLIC),
        AAD_APPLICATION_KEY(ClientApplicationType.CONFIDENTIAL),
        AAD_APPLICATION_CERTIFICATE(ClientApplicationType.CONFIDENTIAL),
        AAD_ACCESS_TOKEN(ClientApplicationType.NOT_APPLICABLE),
        AAD_ACCESS_TOKEN_PROVIDER(ClientApplicationType.NOT_APPLICABLE);

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

    AadAuthenticationHelper(@NotNull ConnectionStringBuilder csb) throws URISyntaxException {
        URI clusterUri = new URI(csb.getClusterUrl());
        clusterUrl = String.format("%s://%s", clusterUri.getScheme(), clusterUri.getHost());
        scope = String.format("%s/%s", clusterUrl, ".default");
        scopes = new HashSet<>();
        scopes.add(scope);

        if (StringUtils.isNotBlank(csb.getApplicationClientId()) && StringUtils.isNotBlank(csb.getApplicationKey())) {
            clientId = csb.getApplicationClientId();
            clientSecret = ClientCredentialFactory.createFromSecret(csb.getApplicationKey());
            authenticationType = AuthenticationType.AAD_APPLICATION_KEY;
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
            if (StringUtils.isNotBlank(csb.getUserUsername())) {
                userUsername = csb.getUserUsername();
            }
            redirectUri = new URI("http://localhost");
            authenticationType = AuthenticationType.AAD_USER_PROMPT;
        }

        aadAuthorityUrl = determineAadAuthorityUrl(csb.getAuthorityId());
    }

    private String determineAadAuthorityUrl(String aadAuthorityUrl) {
        String aadAuthorityId = (aadAuthorityUrl == null ? ORGANIZATION_URI_SUFFIX : aadAuthorityUrl);
        String aadAuthorityFromEnv = System.getenv("AadAuthorityUri");
        if (aadAuthorityFromEnv == null) {
            aadAuthorityUrl = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
        } else {
            aadAuthorityUrl = String.format("%s%s%s", aadAuthorityFromEnv, aadAuthorityFromEnv.endsWith("/") ? "" : "/", aadAuthorityId);
        }
        return aadAuthorityUrl;
    }

    protected String acquireAccessToken() throws DataServiceException, DataClientException {
        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN) {
            return accessToken;
        }

        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN_PROVIDER) {
            try {
                Future<String> future = executor.submit(tokenProvider);
                return getAsyncResultNonblocking(future);
            } catch (Exception e) {
                throw new DataClientException(clusterUrl, e.getMessage(), e);
            }
        }

		// There's a potential race condition here, but it's rare and it was decided that it'd be worse to add a lock
        if (lastAuthenticationResult == null) {
            lastAuthenticationResult = acquireNewAccessToken();
        } else {
            lastAuthenticationResult = acquireAccessTokenSilently();
        }

        return lastAuthenticationResult.accessToken();
    }

    protected IAuthenticationResult acquireNewAccessToken() throws DataServiceException, DataClientException {
        switch (authenticationType) {
            case AAD_APPLICATION_KEY:
                return acquireWithAadApplicationKey();
            case AAD_APPLICATION_CERTIFICATE:
                return acquireWithAadApplicationClientCertificate();
            case AAD_USER_PROMPT:
                return acquireWithUserPrompt();
            default:
                throw new DataClientException(String.format("Authentication type '%s' is invalid (cluster URL '%s')", authenticationType.name(), clusterUrl));
        }
    }

    protected IAuthenticationResult acquireWithAadApplicationKey() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            confidentialClientApplication = ConfidentialClientApplication.builder(clientId, clientSecret).authority(aadAuthorityUrl).build();
            CompletableFuture<IAuthenticationResult> future = confidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            /*
             * TODO: These 5 CompletableFuture.get() calls are blocking. Ideally, I think this API should return the
             *  CompletableFuture so that this will be an async method and let the SDK user wait for the value per their
             *  needs, though I doubt others would find it worthwhile. Else, I can unblock the thread with sleep(exponentialBackoff)
             *  as I proposed with AAD_ACCESS_TOKEN_PROVIDER, but I doubt others would find that worthwhile either.
             */
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireWithAadApplicationKey got 'null' authentication result");
        }
        return result;
    }

    protected IAuthenticationResult acquireWithAadApplicationClientCertificate() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            confidentialClientApplication = ConfidentialClientApplication.builder(applicationClientId, clientCertificate).authority(aadAuthorityUrl).validateAuthority(false).build();
            CompletableFuture<IAuthenticationResult> future = confidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireWithAadApplicationClientCertificate got 'null' authentication result");
        }
        return result;
    }

    protected IAuthenticationResult acquireWithUserPrompt() throws DataServiceException, DataClientException {
        IAuthenticationResult result;
        try {
            publicClientApplication = PublicClientApplication.builder(CLIENT_ID).authority(aadAuthorityUrl).build();
            CompletableFuture<IAuthenticationResult> future = publicClientApplication.acquireToken(InteractiveRequestParameters.builder(redirectUri).scopes(scopes).loginHint(userUsername).build());
            result = future.get(USER_PROMPT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        }
        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireWithUserPrompt got 'null' authentication result");
        }
        return result;
    }

    protected IAuthenticationResult acquireAccessTokenSilently() throws DataServiceException, DataClientException {
        CompletableFuture<Set<IAccount>> accounts;
        try {
            switch (authenticationType.getClientApplicationType()) {
                case CONFIDENTIAL:
                    accounts = confidentialClientApplication.getAccounts();
                    return confidentialClientApplication.acquireTokenSilently(getSilentParameters(accounts)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                case PUBLIC:
                    accounts = publicClientApplication.getAccounts();
                    return publicClientApplication.acquireTokenSilently(getSilentParameters(accounts)).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                case NOT_APPLICABLE:
                    throw new DataClientException("Cannot obtain a refresh token for Authentication type '" + authenticationType.name());
                default:
                    throw new DataClientException("Authentication type '" + authenticationType.name() + "' is invalid");
            }
        } catch (MalformedURLException e) {
            throw new DataClientException(clusterUrl, ERROR_INVALID_AUTHORITY_URL, e);
        } catch (TimeoutException | ExecutionException e) {
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DataServiceException(clusterUrl, ERROR_ACQUIRING_APPLICATION_ACCESS_TOKEN, e);
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