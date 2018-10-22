package com.microsoft.azure.kusto.data;

import com.microsoft.aad.adal4j.AsymmetricKeyCredential;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class AadAuthenticationHelper {

    private final static String DEFAULT_AAD_TENANT = "common";
    private final static String CLIENT_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58";
    private final static String RESOURCE = "https://graph.windows.net";

    private ClientCredential clientCredential;
    private String userUsername;
    private String userPassword;
    private String clusterUrl;
    private String aadAuthorityUri;

    AadAuthenticationHelper(ConnectionStringBuilder csb) {
        clusterUrl = csb.getClusterUrl();

        if (!isNullOrEmpty(csb.getApplicationClientId()) && !isNullOrEmpty(csb.getApplicationKey())) {
            clientCredential = new ClientCredential(csb.getApplicationClientId(), csb.getApplicationKey());
        } else {
            userUsername = csb.getUserUsername();
            userPassword = csb.getUserPassword();
        }

        // Set the AAD Authority URI
        String aadAuthorityId = (csb.getAuthorityId() == null ? DEFAULT_AAD_TENANT : csb.getAuthorityId());
        aadAuthorityUri = String.format("https://login.microsoftonline.com/%s", aadAuthorityId);
    }

    String acquireAccessToken() throws DataServiceException, DataClientException {
        if (clientCredential != null) {
            return acquireAadApplicationAccessToken().getAccessToken();
        } else {
            return acquireAadUserAccessToken().getAccessToken();
        }
    }

    private AuthenticationResult acquireAadUserAccessToken() throws DataServiceException, DataClientException {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(aadAuthorityUri, true, service);

            Future<AuthenticationResult> future = context.acquireToken(
                    clusterUrl, CLIENT_ID, userUsername, userPassword,
                    null);
            result = future.get();
        } catch (InterruptedException | ExecutionException | MalformedURLException e) {
            throw new DataClientException(clusterUrl, "Error in acquiring UserAccessToken", e);
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireAadUserAccessToken got 'null' authentication result");
        }
        return result;
    }

    private AuthenticationResult acquireAadApplicationAccessToken() throws DataServiceException, DataClientException {
        AuthenticationContext context;
        AuthenticationResult result;
        ExecutorService service = null;
        try {
            service = Executors.newFixedThreadPool(1);
            context = new AuthenticationContext(aadAuthorityUri, true, service);
            Future<AuthenticationResult> future = context.acquireToken(clusterUrl, clientCredential, null);
            result = future.get();
        } catch (InterruptedException | ExecutionException | MalformedURLException e) {
            throw new DataClientException(clusterUrl, "Error in acquiring ApplicationAccessToken", e);
        } finally {
            if (service != null) {
                service.shutdown();
            }
        }

        if (result == null) {
            throw new DataServiceException(clusterUrl, "acquireAadApplicationAccessToken got 'null' authentication result");
        }
        return result;
    }

    private Boolean isNullOrEmpty(String str) {
        return (str == null || str.trim().isEmpty());
    }

    public AuthenticationResult acquireWithClientCertificate(String certPath, String keyPath, String pemPassword)
            throws IOException, CertificateException, OperatorCreationException, PKCSException,
            InterruptedException, ExecutionException{
        final KeyCert certificateKey = readPem(certPath, pemPassword);

        final PrivateKey privateKey = readPem(keyPath, pemPassword).getKey();

        AuthenticationContext context;
        context = new AuthenticationContext(aadAuthorityUri, false, Executors.newFixedThreadPool(1));
        AsymmetricKeyCredential asymmetricKeyCredential = AsymmetricKeyCredential.create(clientCredential.getClientId(),
                privateKey, certificateKey.getCertificate());
        // pass null value for optional callback function and acquire access token
        AuthenticationResult result = context.acquireToken(clusterUrl, asymmetricKeyCredential, null).get();

        return result;
    }

    private static KeyCert readPem(String path, String password) throws IOException, CertificateException, OperatorCreationException, PKCSException {

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
}