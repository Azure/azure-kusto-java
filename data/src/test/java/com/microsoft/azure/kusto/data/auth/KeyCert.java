// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

public class KeyCert {
    private X509Certificate certificate;
    private PrivateKey key;

    KeyCert(X509Certificate certificate, PrivateKey key) {
        this.certificate = certificate;
        this.key = key;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    void setCertificate(X509Certificate certificate) {
        this.certificate = certificate;
    }

    public PrivateKey getKey() {
        return key;
    }

    void setKey(PrivateKey key) {
        this.key = key;
    }
}
