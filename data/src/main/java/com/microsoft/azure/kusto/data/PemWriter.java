package com.microsoft.azure.kusto.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Writes a certificate and private key to a PEM file.
 * Yes, there is no built-in way to do this in Java.
 */
public class PemWriter {
    final static Charset UTF_8 = StandardCharsets.UTF_8;
    final static byte[] BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----".getBytes(UTF_8);
    final static byte[] END_CERTIFICATE = "-----END CERTIFICATE-----".getBytes(UTF_8);
    final static byte[] BEGIN_PRIVATE_kEY = "-----BEGIN PRIVATE KEY-----".getBytes(UTF_8);
    final static byte[] END_PRIVATE_kEY = "-----END PRIVATE KEY-----".getBytes(UTF_8);
    final static byte[] NEWLINE = "\n".getBytes(UTF_8);

    public static byte[] encodeToPem(byte @NotNull [] certificatePrivateKey, @Nullable List<X509Certificate> x509CertificateChain) throws CertificateEncodingException {
        ByteBuf sb = Unpooled.buffer();
        sb.writeBytes(BEGIN_PRIVATE_kEY).writeBytes(NEWLINE);
        sb.writeBytes(certificatePrivateKey).writeBytes(NEWLINE);
        sb.writeBytes(END_PRIVATE_kEY).writeBytes(NEWLINE);

        if (x509CertificateChain != null) {
            for (X509Certificate cert : x509CertificateChain) {
                sb.writeBytes(BEGIN_CERTIFICATE).writeBytes(NEWLINE);
                sb.writeBytes(cert.getEncoded()).writeBytes(NEWLINE);
                sb.writeBytes(END_CERTIFICATE).writeBytes(NEWLINE);
            }
        }

        return sb.array();
    }
}
