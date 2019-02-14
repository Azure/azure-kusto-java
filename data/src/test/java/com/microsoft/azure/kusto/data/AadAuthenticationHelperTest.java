package com.microsoft.azure.kusto.data;


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
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class AadAuthenticationHelperTest {

    private AadAuthenticationHelper aadAuthenticationHelper = spy(AadAuthenticationHelper.class);


    @Test
    @DisplayName("validate auth with certificate throws exception when missing or invalid parameters")
    void acquireWithClientCertificateNullKey() throws CertificateException, OperatorCreationException,
            PKCSException, IOException, URISyntaxException {

        String certFilePath = Paths.get("src","test","resources", "cert.cer").toString();
        String privateKeyPath = Paths.get("src","test","resources","key.pem").toString();

        X509Certificate x509Certificate = readPem(certFilePath, "basic").getCertificate();
        PrivateKey privateKey = readPem(privateKeyPath, "basic").getKey();

        ConnectionStringBuilder csb = ConnectionStringBuilder
                .createWithAadApplicationCertificate("resource.uri", "client-id", x509Certificate, privateKey);

        AadAuthenticationHelper aadAuthenticationHelper = new AadAuthenticationHelper(csb);

        Assertions.assertThrows(ExecutionException.class,
                () -> aadAuthenticationHelper.acquireWithClientCertificate());
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
    @DisplayName("validate auth with cached token")
    void useCachedTokenAndRefreshWhenNeeded(){
        //if less than minute - he will refresh
        //try override
        doReturn(ingestionResultMock).when(ingestClientSpy).ingestFromFile(any(), any());

    }
}
