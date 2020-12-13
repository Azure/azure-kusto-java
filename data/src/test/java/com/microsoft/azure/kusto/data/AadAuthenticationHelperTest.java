// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.ITenantProfile;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.microsoft.azure.kusto.data.AadAuthenticationHelper.MIN_ACCESS_TOKEN_VALIDITY_IN_MILLISECS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class AadAuthenticationHelperTest {
    @Test
    @DisplayName("validate auth with certificate throws exception when missing or invalid parameters")
    void acquireWithClientCertificateNullKey() throws CertificateException, OperatorCreationException,
            PKCSException, IOException, URISyntaxException {
        String certFilePath = Paths.get("src", "test", "resources", "cert.cer").toString();
        String privateKeyPath = Paths.get("src", "test", "resources", "key.pem").toString();

        X509Certificate x509Certificate = readPem(certFilePath, "basic").getCertificate();
        PrivateKey privateKey = readPem(privateKeyPath, "basic").getKey();

        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCertificate("resource.uri", "client-id", x509Certificate, privateKey);

        AadAuthenticationHelper aadAuthenticationHelper = new AadAuthenticationHelper(csb);

        Assertions.assertThrows(DataServiceException.class,
                () -> aadAuthenticationHelper.acquireWithAadApplicationClientCertificate());
    }

    static KeyCert readPem(String path, String password)
            throws IOException, CertificateException, OperatorCreationException, PKCSException {

        Security.addProvider(new BouncyCastleProvider());
        PEMParser pemParser = new PEMParser(new FileReader(new File(path)));
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
    void useCachedTokenAndRefreshWhenNeeded() throws InterruptedException, ExecutionException, IOException,
            DataServiceException, URISyntaxException, CertificateException, OperatorCreationException, PKCSException, DataClientException, TimeoutException {
        String certFilePath = Paths.get("src", "test", "resources", "cert.cer").toString();
        String privateKeyPath = Paths.get("src", "test", "resources", "key.pem").toString();

        X509Certificate x509Certificate = readPem(certFilePath, "basic").getCertificate();
        PrivateKey privateKey = readPem(privateKeyPath, "basic").getKey();

        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCertificate("resource.uri", "client-id", x509Certificate, privateKey);

        AadAuthenticationHelper aadAuthenticationHelperSpy = spy(new AadAuthenticationHelper(csb));

        IAuthenticationResult authenticationResult = new MockAuthenticationResult("firstToken", "firstToken", new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date());
        IAuthenticationResult authenticationResultFromRefresh = new MockAuthenticationResult("fromRefresh", "fromRefresh", new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date());
        IAuthenticationResult authenticationResultNullRefreshTokenResult = new MockAuthenticationResult("nullRefreshResult", "nullRefreshResult", new MockAccount("homeAccountId", "environment", "username", Collections.emptyMap()), "environment", "environment", new Date());

        doReturn(authenticationResultFromRefresh).when(aadAuthenticationHelperSpy).acquireAccessTokenSilently();
        doReturn(authenticationResult).when(aadAuthenticationHelperSpy).acquireWithAadApplicationClientCertificate();

        assertEquals("firstToken", aadAuthenticationHelperSpy.acquireAccessToken());

        // Token was passed as expired - expected to be refreshed
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken());

        // Token is still valid - expected to return the same
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken());

        doReturn(authenticationResultNullRefreshTokenResult).when(aadAuthenticationHelperSpy).acquireWithAadApplicationClientCertificate();

        // Null refresh token + token is now expired- expected to authenticate again and reacquire token
        assertEquals("fromRefresh", aadAuthenticationHelperSpy.acquireAccessToken());
    }

    class MockAccount implements IAccount {
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

    class MockAuthenticationResult implements IAuthenticationResult {
        private final String accessToken;
        private final String idToken;
        private final MockAccount account;
        private final String environment;
        private final String scopes;
        private final Date expiresOnDate;

        public MockAuthenticationResult(String accessToken, String idToken, MockAccount account, String environment, String scopes, Date expiresOnDate) {
            this.accessToken = accessToken;
            this.idToken = idToken;
            this.account = account;
            this.environment = environment;
            this.scopes = scopes;
            this.expiresOnDate = expiresOnDate;
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
