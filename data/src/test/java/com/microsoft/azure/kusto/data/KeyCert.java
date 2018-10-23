package com.microsoft.azure.kusto.data;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

class KeyCert {

    private X509Certificate certificate;
    private PrivateKey key;

    KeyCert(X509Certificate certificate, PrivateKey key) {
        this.certificate = certificate;
        this.key = key;
    }

    X509Certificate getCertificate() {
        return certificate;
    }

    void setCertificate(X509Certificate certificate) {
        this.certificate = certificate;
    }

    PrivateKey getKey() {
        return key;
    }

    void setKey(PrivateKey key) {
        this.key = key;
    }

}
