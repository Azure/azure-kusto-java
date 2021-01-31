// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCSException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.microsoft.azure.kusto.data.auth.AadAuthenticationHelperTest.readPem;

class ConnectionStringBuilderTest {
    @Test
    @DisplayName("validate createWithAadApplicationCredentials throws IllegalArgumentException exception when missing or invalid parameters")
    void createWithAadApplicationCredentials() {

        //nullOrEmpty appId
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCredentials("resource.uri", null, "appKey"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCredentials("resource.uri", "", "appKey"));
        //nullOrEmpty appKey
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCredentials("resource.uri", "appId", null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCredentials("resource.uri", "appId", ""));
        //nullOrEmpty resourceUri
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCredentials(null, "appId", "appKey"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCredentials("", "appId", "appKey"));
    }

    @Test
    @DisplayName("validate createWithUserPrompt throws IllegalArgumentException exception when missing or invalid parameters")
    void createWithUserPrompt() {
        //nullOrEmpty resourceUri
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithUserPrompt(null, null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithUserPrompt("", ""));
    }

    @Test
    @DisplayName("validate createWithAadApplicationCertificate throws IllegalArgumentException exception when missing or invalid parameters")
    void createWithAadApplicationCertificate() throws CertificateException, OperatorCreationException,
            PKCSException, IOException {

        String certFilePath = Paths.get("src", "test", "resources", "cert.cer").toString();
        String privateKeyPath = Paths.get("src", "test", "resources", "key.pem").toString();

        X509Certificate x509Certificate = readPem(certFilePath, "basic").getCertificate();
        PrivateKey privateKey = readPem(privateKeyPath, "basic").getKey();

        //nullOrEmpty resourceUri
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCertificate(null, "appId", x509Certificate, privateKey));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCertificate("", "appId", x509Certificate, privateKey));

        //nullOrEmpty appId
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCertificate("resource.uri", null, x509Certificate, privateKey));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCertificate("resource.uri", "", x509Certificate, privateKey));
        //null certificate
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCertificate("resource.uri", "appID", null, privateKey));
        //null privateKey
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadApplicationCertificate("resource.uri", "appID", x509Certificate, null));
    }

    @Test
    @DisplayName("validate createWithAadAccessTokenAuthentication throws IllegalArgumentException exception when missing or invalid parameters")
    void createWithAadAccessTokenAuthentication() {

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadAccessTokenAuthentication(null, "token"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadAccessTokenAuthentication("", "token"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadAccessTokenAuthentication("resource.uri", null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadAccessTokenAuthentication("resource.uri", ""));
        Assertions.assertDoesNotThrow(() -> ConnectionStringBuilder
                .createWithAadAccessTokenAuthentication("resource.uri", "token"));
    }

    @Test
    @DisplayName("validate createWithAadTokenProviderAuthentication throws IllegalArgumentException exception when missing or invalid parameters")
    void createWithAadTokenProviderAuthentication() {

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadTokenProviderAuthentication(null, () -> "token"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadTokenProviderAuthentication("", () -> "token"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectionStringBuilder
                        .createWithAadTokenProviderAuthentication("resource.uri", null));
        Assertions.assertDoesNotThrow(() -> ConnectionStringBuilder
                .createWithAadTokenProviderAuthentication("resource.uri", () -> "token"));
    }

    @Test
    @DisplayName("validate ClientImpl strips fed=true if needed")
    void stripFederatedAuthFromCSB() throws URISyntaxException {
        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCredentials("https://service.uri;fed=true", "id", "appKey");
        ClientImpl client = new ClientImpl(csb);
        Assertions.assertEquals("https://service.uri", csb.getClusterUrl());

        String clusterUrl = "https://service.uri";
        csb = ConnectionStringBuilder.createWithAadApplicationCredentials(clusterUrl, "id", "appKey");
        client = new ClientImpl(csb);
        Assertions.assertEquals(clusterUrl, csb.getClusterUrl());
    }
}