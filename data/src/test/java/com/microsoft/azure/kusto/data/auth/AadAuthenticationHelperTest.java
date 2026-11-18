// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.InteractiveBrowserCredential;
import com.microsoft.aad.msal4j.AuthenticationResultMetadata;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCertificate;
import com.microsoft.aad.msal4j.IConfidentialClientApplication;
import com.microsoft.aad.msal4j.ITenantProfile;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import reactor.core.publisher.Mono;

public class AadAuthenticationHelperTest {
    @BeforeAll
    public static void setUp() throws URISyntaxException {
        CloudInfo.manuallyAddToCache("https://resource.uri", Mono.just(CloudInfo.DEFAULT_CLOUD));
    }

    @Test
    @DisplayName("validate auth with certificate wraps token acquisition failures")
    void acquireWithClientCertificateFailure() throws MalformedURLException, URISyntaxException {
        IConfidentialClientApplication clientApplication = mock(IConfidentialClientApplication.class);
        IllegalStateException tokenFailure = new IllegalStateException("Token acquisition failed");
        when(clientApplication.acquireToken(any(ClientCredentialParameters.class)))
                .thenReturn(CompletableFuture.failedFuture(tokenFailure));
        ApplicationCertificateTokenProvider aadAuthenticationHelper = createCertificateTokenProvider(clientApplication);

        aadAuthenticationHelper.initialize().block();
        assertEquals("https://login.microsoftonline.com/organizations/", aadAuthenticationHelper.aadAuthorityUrl);
        assertEquals(new HashSet<>(Collections.singletonList("https://kusto.kusto.windows.net/.default")), aadAuthenticationHelper.scopes);
        DataServiceException exception = Assertions.assertThrows(DataServiceException.class,
                aadAuthenticationHelper::acquireNewAccessToken);
        assertSame(tokenFailure, exception.getCause().getCause());
    }

    private static ApplicationCertificateTokenProvider createCertificateTokenProvider(IConfidentialClientApplication clientApplication)
            throws MalformedURLException, URISyntaxException {
        X509Certificate certificate = mock(X509Certificate.class);
        PrivateKey privateKey = mock(PrivateKey.class);
        IClientCertificate clientCertificate = mock(IClientCertificate.class);
        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCertificate("https://resource.uri", "client-id", certificate, privateKey);
        try (MockedStatic<ClientCredentialFactory> credentialFactory = mockStatic(ClientCredentialFactory.class)) {
            credentialFactory.when(() -> ClientCredentialFactory.createFromCertificate(privateKey, certificate))
                    .thenReturn(clientCertificate);
            ApplicationCertificateTokenProvider provider = spy((ApplicationCertificateTokenProvider) TokenProviderFactory.createTokenProvider(csb, null));
            credentialFactory.verify(() -> ClientCredentialFactory.createFromCertificate(privateKey, certificate));
            doReturn(clientApplication).when(provider).getClientApplication();
            return provider;
        }
    }

    @Test
    @DisplayName("validate cached token. Refresh if needed. Call regularly if no refresh token")
    void useCachedTokenAndRefreshWhenNeeded()
            throws MalformedURLException, DataServiceException, URISyntaxException, DataClientException {
        ApplicationCertificateTokenProvider aadAuthenticationHelperSpy = createCertificateTokenProvider(mock(IConfidentialClientApplication.class));

        IAuthenticationResult authenticationResult = new MockAuthenticationResult("firstToken", "firstToken",
                new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date(),
                Mockito.mock(ITenantProfile.class));
        IAuthenticationResult authenticationResultFromRefresh = new MockAuthenticationResult("fromRefresh", "fromRefresh",
                new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date(),
                Mockito.mock(ITenantProfile.class));
        IAuthenticationResult authenticationResultNullRefreshTokenResult = new MockAuthenticationResult("nullRefreshResult", "nullRefreshResult",
                new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date(),
                Mockito.mock(ITenantProfile.class));

        // doThrow(DataServiceException.class).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        doReturn(null).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        doReturn(authenticationResult).when(aadAuthenticationHelperSpy).acquireNewAccessToken();
        assertEquals("firstToken", aadAuthenticationHelperSpy.acquireAccessToken().block());
        assertEquals("https://login.microsoftonline.com/organizations/", aadAuthenticationHelperSpy.aadAuthorityUrl);
        assertEquals(new HashSet<>(Collections.singletonList("https://kusto.kusto.windows.net/.default")), aadAuthenticationHelperSpy.scopes);

        doReturn(authenticationResultFromRefresh).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        // Token was passed as expired - expected to be refreshed
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken().block());
        // Token is still valid - expected to return the same
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken().block());

        doReturn(authenticationResultNullRefreshTokenResult).when(aadAuthenticationHelperSpy).acquireNewAccessToken();
        // Null refresh token + token is now expired- expected to authenticate again and reacquire token
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken().block());
    }

    @Test
    @DisplayName("validate cloud settings for non-standard cloud")
    void checkCloudSettingsAbnormal() throws URISyntaxException, IllegalAccessException, NoSuchFieldException {

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("https://weird.resource.uri", "81f9bae6-35c3-44bc-b116-e6305a4d8fdd", "");

        UserPromptTokenProvider aadAuthenticationHelper = (UserPromptTokenProvider) TokenProviderFactory.createTokenProvider(csb, null);
        CloudInfo.manuallyAddToCache("https://weird.resource.uri", Mono.just(new CloudInfo(
                true,
                "https://nostandard-login-input",
                "non_standard_client_id",
                "",
                "https://aaaa.kusto.bbbb.com",
                "first_party_url"

        )));

        aadAuthenticationHelper.initialize().block();

        // To keep this test we have to do reflection to get the private fields

        final InteractiveBrowserCredential cred = (InteractiveBrowserCredential) aadAuthenticationHelper.cred;
        final TokenRequestContext tokenRequestContext = aadAuthenticationHelper.tokenRequestContext;

        String authorityUrl = "https://nostandard-login-input/81f9bae6-35c3-44bc-b116-e6305a4d8fdd/";
        // compare to cred.authority()
        Field authorityHost = cred.getClass().getDeclaredField("authorityHost");
        authorityHost.setAccessible(true);
        assertEquals(authorityUrl, authorityHost.get(cred));

        assertEquals(Collections.singletonList("https://aaaa.kustomfa.bbbb.com/.default"), tokenRequestContext.getScopes());
    }

    @Test
    @DisplayName("validate cloud settings for the standard cloud")
    void checkCloudSettingsNormal() throws URISyntaxException, NoSuchFieldException, IllegalAccessException {

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("https://normal.resource.uri", "91f9bae6-35c3-44bc-b116-e6305a4d8fdd", "");

        UserPromptTokenProvider aadAuthenticationHelper = (UserPromptTokenProvider) TokenProviderFactory.createTokenProvider(csb, null);
        CloudInfo.manuallyAddToCache("https://normal.resource.uri", Mono.just(CloudInfo.DEFAULT_CLOUD));

        aadAuthenticationHelper.initialize().block();

        // To keep this test we have to do reflection to get the private fields

        final InteractiveBrowserCredential cred = (InteractiveBrowserCredential) aadAuthenticationHelper.cred;
        final TokenRequestContext tokenRequestContext = aadAuthenticationHelper.tokenRequestContext;

        String authorityUrl = CloudInfo.DEFAULT_PUBLIC_LOGIN_URL + "/91f9bae6-35c3-44bc-b116-e6305a4d8fdd/";
        // compare to cred.authority()
        Field authorityHost = cred.getClass().getDeclaredField("authorityHost");
        authorityHost.setAccessible(true);
        assertEquals(authorityUrl, authorityHost.get(cred));

        assertEquals(Collections.singletonList(CloudInfo.DEFAULT_KUSTO_SERVICE_RESOURCE_ID + "/.default"), tokenRequestContext.getScopes());

    }

    static class MockAccount implements IAccount {
        private final String homeAccountId;
        private final String environment;
        private final String username;
        private final Map<String, ITenantProfile> getTenantProfiles;

        public MockAccount(String homeAccountId, String environment, String username, Map<String, ITenantProfile> getTenantProfiles) {
            this.homeAccountId = homeAccountId;
            this.environment = environment;
            this.username = username;
            this.getTenantProfiles = getTenantProfiles;
        }

        @Override
        public String homeAccountId() {
            return homeAccountId;
        }

        @Override
        public String environment() {
            return environment;
        }

        @Override
        public String username() {
            return username;
        }

        @Override
        public Map<String, ITenantProfile> getTenantProfiles() {
            return getTenantProfiles;
        }
    }

    static class MockAuthenticationResult implements IAuthenticationResult {
        private final String accessToken;
        private final String idToken;
        private final MockAccount account;
        private final String environment;
        private final String scopes;
        private final Date expiresOnDate;
        private final ITenantProfile tenantProfile;

        public MockAuthenticationResult(String accessToken, String idToken, MockAccount account, String environment, String scopes, Date expiresOnDate,
                ITenantProfile tenantProfile) {
            this.accessToken = accessToken;
            this.idToken = idToken;
            this.account = account;
            this.environment = environment;
            this.scopes = scopes;
            this.expiresOnDate = expiresOnDate;
            this.tenantProfile = tenantProfile;
        }

        @Override
        public String accessToken() {
            return accessToken;
        }

        @Override
        public String idToken() {
            return idToken;
        }

        @Override
        public IAccount account() {
            return account;
        }

        @Override
        public ITenantProfile tenantProfile() {
            return tenantProfile;
        }

        @Override
        public String environment() {
            return environment;
        }

        @Override
        public String scopes() {
            return scopes;
        }

        @Override
        public Date expiresOnDate() {
            return expiresOnDate;
        }

        @Override
        public AuthenticationResultMetadata metadata() {
            return null;
        }
    }
}
