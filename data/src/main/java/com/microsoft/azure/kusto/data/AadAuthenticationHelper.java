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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

class AadAuthenticationHelper {
    protected static final String ORGANIZATION_URI_SUFFIX = "organizations";
    // TODO: Get ClientId from CM endpoint
    protected static final String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    protected static final int MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS = 60 * 1000;
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

        aadAuthorityUrl = determineAadAuthorityUrl(csb.getAuthorityId());

        try {
            if (StringUtils.isNotBlank(csb.getApplicationClientId()) && StringUtils.isNotBlank(csb.getApplicationKey())) {
                clientId = csb.getApplicationClientId();
                clientSecret = ClientCredentialFactory.createFromSecret(csb.getApplicationKey());
                confidentialClientApplication = ConfidentialClientApplication.builder(clientId, clientSecret).authority(aadAuthorityUrl).build();
                authenticationType = AuthenticationType.AAD_APPLICATION_KEY;
            } else if (csb.getX509Certificate() != null && csb.getPrivateKey() != null && StringUtils.isNotBlank(csb.getApplicationClientId())) {
                clientCertificate = ClientCredentialFactory.createFromCertificate(csb.getPrivateKey(), csb.getX509Certificate());
                applicationClientId = csb.getApplicationClientId();
                confidentialClientApplication = ConfidentialClientApplication.builder(applicationClientId, clientCertificate).authority(aadAuthorityUrl).validateAuthority(false).build();
                authenticationType = AuthenticationType.AAD_APPLICATION_CERTIFICATE;
            } else if (StringUtils.isNotBlank(csb.getAccessToken())) {
                accessToken = csb.getAccessToken();
                authenticationType = AuthenticationType.AAD_ACCESS_TOKEN;
            } else if (csb.getTokenProvider() != null) {
                tokenProvider = csb.getTokenProvider();
                authenticationType = AuthenticationType.AAD_ACCESS_TOKEN_PROVIDER;
            } else {
                if (StringUtils.isNotBlank(csb.getUserUsernameHint())) {
                    userUsername = csb.getUserUsernameHint();
                }
                redirectUri = new URI("http://localhost");
                publicClientApplication = PublicClientApplication.builder(CLIENT_ID).authority(aadAuthorityUrl).build();
                authenticationType = AuthenticationType.AAD_USER_PROMPT;
            }
        } catch (MalformedURLException e) {
            throw new URISyntaxException(aadAuthorityUrl, ERROR_INVALID_AUTHORITY_URL);
        }
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

    protected String acquireAccessToken() throws DataServiceException, DataClientException {
        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN) {
            return accessToken;
        }

        if (authenticationType == AuthenticationType.AAD_ACCESS_TOKEN_PROVIDER) {
            try {
                Future<String> future = executor.submit(tokenProvider);
                return future.get(USER_PROMPT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new DataClientException(clusterUrl, e.getMessage(), e);
            }
        }

        try {
            return acquireAccessTokenSilently().accessToken();
        } catch (DataServiceException ex) {
            logger.error("Failed to acquire access token silently (via cache or refresh token). Attempting to get new access token.", ex);
            return acquireNewAccessToken().accessToken();
        }
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

    protected IAuthenticationResult acquireWithAadApplicationKey() throws DataServiceException {
        IAuthenticationResult result;
        try {
            CompletableFuture<IAuthenticationResult> future = confidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
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

    protected IAuthenticationResult acquireWithAadApplicationClientCertificate() throws DataServiceException {
        IAuthenticationResult result;
        try {
            CompletableFuture<IAuthenticationResult> future = confidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build());
            result = future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
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
            // This is the only auth method that allows the same application to be used for multiple distinct accounts, so reset account cache between sign-ins
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
        Set<IAccount> accountSet = accounts.join();
        if (StringUtils.isNotBlank(userUsername)) {
            account = accountSet.stream().filter(u -> userUsername.equalsIgnoreCase(u.username())).findAny().orElse(null);
        } else {
            if (accountSet.isEmpty()) {
                account = null;
            } else {
                // Normally we would filter accounts by the user authenticating, but there's only 1 per AadAuthenticationHelper instance
                account = accountSet.iterator().next();
            }
        }
        if (account == null) {
            return SilentParameters.builder(scopes).authorityUrl(aadAuthorityUrl).build();
        }
        return SilentParameters.builder(scopes).account(account).authorityUrl(aadAuthorityUrl).build();
    }
}