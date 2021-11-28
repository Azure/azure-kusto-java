// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.ITenantProfile;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class AadAuthenticationHelperTest {
    @BeforeAll
    public static void setUp() {
        CloudInfo.manuallyAddToCache("https://resource.uri", CloudInfo.DEFAULT_CLOUD);
    }

    @Test
    @DisplayName("validate auth with certificate throws exception when missing or invalid parameters")
    void acquireWithClientCertificateNullKey() throws CertificateException, OperatorCreationException,
            PKCSException, IOException, URISyntaxException, DataServiceException, DataClientException {
        String certFilePath = Paths.get("src", "test", "resources", "cert.cer").toString();
        String privateKeyPath = Paths.get("src", "test", "resources", "key.pem").toString();

        X509Certificate x509Certificate = readPem(certFilePath, "basic").getCertificate();
        PrivateKey privateKey = readPem(privateKeyPath, "basic").getKey();

        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCertificate("https://resource.uri", "client-id", x509Certificate, privateKey);

        MsalTokenProviderBase aadAuthenticationHelper = (MsalTokenProviderBase) TokenProviderFactory.createTokenProvider(csb);

        aadAuthenticationHelper.initializeCloudInfo();
        aadAuthenticationHelper.setRequiredMembersBasedOnCloudInfo();
        assertEquals("https://login.microsoftonline.com/organizations/", aadAuthenticationHelper.aadAuthorityUrl);
        assertEquals(new HashSet<>(Collections.singletonList("https://kusto.kusto.windows.net/.default")), aadAuthenticationHelper.scopes);

        Assertions.assertThrows(DataServiceException.class, aadAuthenticationHelper::acquireNewAccessToken);
    }

    public static KeyCert readPem(String path, String password)
            throws IOException, CertificateException, OperatorCreationException, PKCSException {

        Security.addProvider(new BouncyCastleProvider());
        PEMParser pemParser = new PEMParser(new FileReader(path));
        PrivateKey privateKey = null;
        X509Certificate cert = null;
        Object object = pemParser.readObject();

        while (object != null) {
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
            if (object instanceof X509CertificateHolder) {
                cert = new JcaX509CertificateConverter().getCertificate((X509CertificateHolder) object);
            }
            if (object instanceof PKCS8EncryptedPrivateKeyInfo) {
                PKCS8EncryptedPrivateKeyInfo pinfo = (PKCS8EncryptedPrivateKeyInfo) object;
                InputDecryptorProvider provider = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password.toCharArray());
                PrivateKeyInfo info = pinfo.decryptPrivateKeyInfo(provider);
                privateKey = converter.getPrivateKey(info);
            }
            if (object instanceof PrivateKeyInfo) {
                privateKey = converter.getPrivateKey((PrivateKeyInfo) object);
            }
            object = pemParser.readObject();
        }

        KeyCert keycert = new KeyCert(null, null);
        keycert.setCertificate(cert);
        keycert.setKey(privateKey);
        pemParser.close();
        return keycert;
    }

    @Test
    @DisplayName("validate cached token. Refresh if needed. Call regularly if no refresh token")
    void useCachedTokenAndRefreshWhenNeeded() throws IOException, DataServiceException, URISyntaxException, CertificateException, OperatorCreationException, PKCSException, DataClientException {
        String certFilePath = Paths.get("src", "test", "resources", "cert.cer").toString();
        String privateKeyPath = Paths.get("src", "test", "resources", "key.pem").toString();

        X509Certificate x509Certificate = readPem(certFilePath, "basic").getCertificate();
        PrivateKey privateKey = readPem(privateKeyPath, "basic").getKey();

        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCertificate("https://resource.uri", "client-id", x509Certificate, privateKey);

        MsalTokenProviderBase aadAuthenticationHelperSpy = (MsalTokenProviderBase) spy(TokenProviderFactory.createTokenProvider(csb));

        IAuthenticationResult authenticationResult = new MockAuthenticationResult("firstToken", "firstToken", new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date(), Mockito.mock(ITenantProfile.class));
        IAuthenticationResult authenticationResultFromRefresh = new MockAuthenticationResult("fromRefresh", "fromRefresh", new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date(), Mockito.mock(ITenantProfile.class));
        IAuthenticationResult authenticationResultNullRefreshTokenResult = new MockAuthenticationResult("nullRefreshResult", "nullRefreshResult", new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date(), Mockito.mock(ITenantProfile.class));

        //doThrow(DataServiceException.class).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        doReturn(null).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        doReturn(authenticationResult).when(aadAuthenticationHelperSpy).acquireNewAccessToken();
        assertEquals("firstToken", aadAuthenticationHelperSpy.acquireAccessToken());
        assertEquals("https://login.microsoftonline.com/organizations/", aadAuthenticationHelperSpy.aadAuthorityUrl);
        assertEquals(new HashSet<>(Collections.singletonList("https://kusto.kusto.windows.net/.default")), aadAuthenticationHelperSpy.scopes);


        doReturn(authenticationResultFromRefresh).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        // Token was passed as expired - expected to be refreshed
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken());
        // Token is still valid - expected to return the same
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken());

        doReturn(authenticationResultNullRefreshTokenResult).when(aadAuthenticationHelperSpy).acquireNewAccessToken();
        // Null refresh token + token is now expired- expected to authenticate again and reacquire token
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken());
    }

    @Test
    @DisplayName("validate cloud settings for non-standard cloud")
    void checkCloudSettingsAbnormal() throws URISyntaxException, DataServiceException, DataClientException {

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("https://weird.resource.uri", "weird_auth_id", "");

        PublicAppTokenProviderBase aadAuthenticationHelper = (PublicAppTokenProviderBase) TokenProviderFactory.createTokenProvider(csb);
        CloudInfo.manuallyAddToCache("https://weird.resource.uri", new CloudInfo(
                true,
                "https://nostandard-login-input",
                "non_standard_client_id",
                "",
                "https://aaaa.kusto.bbbb.com",
                "first_party_url"

        ));

        aadAuthenticationHelper.initializeCloudInfo();
        aadAuthenticationHelper.setRequiredMembersBasedOnCloudInfo();
        assertEquals("non_standard_client_id", aadAuthenticationHelper.clientApplication.clientId());
        assertEquals("https://nostandard-login-input/weird_auth_id/", aadAuthenticationHelper.clientApplication.authority());

        assertEquals("https://nostandard-login-input/weird_auth_id/", aadAuthenticationHelper.aadAuthorityUrl);
        HashSet<String> scopes = new HashSet<>(Collections.singletonList("https://aaaa.kustomfa.bbbb.com/.default"));
        assertEquals(scopes, aadAuthenticationHelper.scopes);

        SilentParameters silentParametersNormalUser = aadAuthenticationHelper.getSilentParameters(
                new HashSet<>(Collections.singletonList(new MockAccount("c0327b6e-814d-4194-8e7f-9fc7a1e5dea9.115d58c9-f699-44e0-8a53-e1861542e510", "", "", null))));
        assertEquals(scopes, silentParametersNormalUser.scopes());
        assertEquals("https://nostandard-login-input/weird_auth_id/", silentParametersNormalUser.authorityUrl());

        SilentParameters silentParametersMsaUser = aadAuthenticationHelper.getSilentParameters(
                new HashSet<>(Collections.singletonList(new MockAccount("c0327b6e-814d-4194-8e7f-9fc7a1e5dea9.9188040d-6c67-4c5b-b112-36a304b66dad", "", "", null))));
        assertEquals(scopes, silentParametersMsaUser.scopes());
        assertEquals("first_party_url", silentParametersMsaUser.authorityUrl());
    }

    @Test
    @DisplayName("validate cloud settings for the standard cloud")
    void checkCloudSettingsNormal() throws URISyntaxException, DataServiceException, DataClientException {

        ConnectionStringBuilder csb = ConnectionStringBuilder.createWithUserPrompt("https://normal.resource.uri", "auth_id", "");

        PublicAppTokenProviderBase aadAuthenticationHelper = (PublicAppTokenProviderBase) TokenProviderFactory.createTokenProvider(csb);
        CloudInfo.manuallyAddToCache("https://normal.resource.uri", CloudInfo.DEFAULT_CLOUD);

        aadAuthenticationHelper.initializeCloudInfo();
        aadAuthenticationHelper.setRequiredMembersBasedOnCloudInfo();
        String authorityUrl = CloudInfo.DEFAULT_PUBLIC_LOGIN_URL + "/auth_id/";
        assertEquals(CloudInfo.DEFAULT_KUSTO_CLIENT_APP_ID, aadAuthenticationHelper.clientApplication.clientId());
        assertEquals(authorityUrl, aadAuthenticationHelper.clientApplication.authority());

        assertEquals(authorityUrl, aadAuthenticationHelper.aadAuthorityUrl);
        HashSet<String> scopes = new HashSet<>(Collections.singletonList(CloudInfo.DEFAULT_KUSTO_SERVICE_RESOURCE_ID + "/.default"));
        assertEquals(scopes, aadAuthenticationHelper.scopes);

        SilentParameters silentParametersNormalUser = aadAuthenticationHelper.getSilentParameters(
                new HashSet<>(Collections.singletonList(new MockAccount("c0327b6e-814d-4194-8e7f-9fc7a1e5dea9.115d58c9-f699-44e0-8a53-e1861542e510", "", "", null))));
        assertEquals(scopes, silentParametersNormalUser.scopes());
        assertEquals(authorityUrl, silentParametersNormalUser.authorityUrl());

        SilentParameters silentParametersMsaUser = aadAuthenticationHelper.getSilentParameters(
                new HashSet<>(Collections.singletonList(new MockAccount("c0327b6e-814d-4194-8e7f-9fc7a1e5dea9.9188040d-6c67-4c5b-b112-36a304b66dad", "", "", null))));
        assertEquals(scopes, silentParametersMsaUser.scopes());
        assertEquals(CloudInfo.DEFAULT_FIRST_PARTY_AUTHORITY_URL, silentParametersMsaUser.authorityUrl());
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

        public MockAuthenticationResult(String accessToken, String idToken, MockAccount account, String environment, String scopes, Date expiresOnDate, ITenantProfile tenantProfile) {
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
    }
}