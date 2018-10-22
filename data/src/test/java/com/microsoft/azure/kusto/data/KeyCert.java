package com.microsoft.azure.kusto.data;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

class KeyCert {

    X509Certificate certificate;
    PrivateKey key;

    public KeyCert(X509Certificate certificate, PrivateKey key) {
        this.certificate = certificate;
        this.key = key;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    public void setCertificate(X509Certificate certificate) {
        this.certificate = certificate;
    }

    public PrivateKey getKey() {
        return key;
    }

    public void setKey(PrivateKey key) {
        this.key = key;
    }

}
