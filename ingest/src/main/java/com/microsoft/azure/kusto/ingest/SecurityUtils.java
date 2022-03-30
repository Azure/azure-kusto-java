// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import java.io.*;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

public class SecurityUtils {
    private SecurityUtils() {
        // Hide constructor as this is a Utils class
    }

    static String removeSecretsFromUrl(String url) {
        return url.split("[?]", 2)[0].split("[;]", 2)[0];
    }

    public static X509Certificate getPublicCertificate(String fileLoc) throws IOException, CertificateException {
        try (FileInputStream is = new FileInputStream(fileLoc)) {
            CertificateFactory fact = CertificateFactory.getInstance("X.509");
            return (X509Certificate) fact.generateCertificate(is);
        }
    }

    public static RSAPrivateKey getPrivateKey(String filename) throws IOException, GeneralSecurityException {
        String privateKeyPEM = getKey(filename);
        return getPrivateKeyFromString(privateKeyPEM);
    }

    public static RSAPublicKey getPublicKey(String filename) throws IOException, GeneralSecurityException {
        String publicKeyPEM = getKey(filename);
        return getPublicKeyFromString(publicKeyPEM);
    }

    private static String getKey(String filename) throws IOException {
        // Read key from file
        StringBuilder strKeyPEM = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
                strKeyPEM.append(line).append("\n");
            }
        }
        return strKeyPEM.toString();
    }

    private static RSAPrivateKey getPrivateKeyFromString(String key) throws GeneralSecurityException {
        String keyPEM = key;
        keyPEM = keyPEM.replace("-----BEGIN PRIVATE KEY-----\n", "");
        keyPEM = keyPEM.replace("-----END PRIVATE KEY-----", "");
        keyPEM = keyPEM.replace("\n", "").replace("\r", "");
        byte[] decoded = java.util.Base64.getDecoder().decode(keyPEM);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
        return (RSAPrivateKey) kf.generatePrivate(keySpec);
    }

    private static RSAPublicKey getPublicKeyFromString(String key) throws GeneralSecurityException {
        String keyPEM = key;
        keyPEM = keyPEM.replace("-----BEGIN PUBLIC KEY-----\n", "");
        keyPEM = keyPEM.replace("-----END PUBLIC KEY-----", "");
        keyPEM = keyPEM.replace("\n", "").replace("\r", "");
        byte[] decoded = java.util.Base64.getDecoder().decode(keyPEM);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(decoded);
        return (RSAPublicKey) kf.generatePublic(keySpec);
    }
}
