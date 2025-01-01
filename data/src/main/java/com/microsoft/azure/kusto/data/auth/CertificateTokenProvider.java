package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.CredentialBuilderBase;
import com.microsoft.azure.kusto.data.PemWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.net.URISyntaxException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.List;

public class CertificateTokenProvider extends AzureIdentityTokenProvider {
    private final byte[] certificatePrivateKey;
    private final List<X509Certificate> x509CertificateChain;
    private final boolean sendCertificateChain;

    CertificateTokenProvider(@NotNull String clusterUrl, @Nullable String tenantId, @Nullable String clientId, @Nullable HttpClient httpClient,
                             byte[] certificatePrivateKey, @Nullable List<X509Certificate> x509CertificateChain, boolean sendCertificateChain) throws URISyntaxException {
        super(clusterUrl, tenantId, clientId, httpClient);
        this.certificatePrivateKey = certificatePrivateKey;
        this.x509CertificateChain = x509CertificateChain;
        this.sendCertificateChain = sendCertificateChain;
    }


    @Override
    protected CredentialBuilderBase<?> initBuilder() {
        try {
            return new ClientCertificateCredentialBuilder()
                    .pemCertificate(new ByteArrayInputStream(PemWriter.encodeToPem(certificatePrivateKey, x509CertificateChain)))
                    .sendCertificateChain(sendCertificateChain);
        } catch (CertificateEncodingException e) {
            throw new RuntimeException(e); // TODO: replace after branch merge with new exception
        }
    }

    @Override
    protected TokenCredential createTokenCredential(CredentialBuilderBase<?> builder) {
        return ((ClientCertificateCredentialBuilder) builder).build();
    }
}
