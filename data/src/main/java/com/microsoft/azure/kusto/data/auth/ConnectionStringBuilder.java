// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import org.apache.commons.lang3.StringUtils;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Callable;

public class ConnectionStringBuilder {
    private String clusterUrl;
    private String usernameHint;
    private String applicationClientId;
    private String applicationKey;
    // Public certificate
    private X509Certificate x509Certificate;
    // Chain comprised of public certificate, its CA's certificate, and the root CA's certificate
    private List<X509Certificate> x509CertificateChain;
    // PEM-encoded private key
    private PrivateKey privateKey;
    // AAD tenant Id (GUID or "microsoft.com")
    private String aadAuthorityId;
    private String accessToken;
    private Callable<String> tokenProvider;
    private String managedIdentityClientId;
    private boolean useDeviceCodeAuth;
    private boolean useManagedIdentityAuth;
    private boolean useUserPromptAuth;
    private String userNameForTracing;
    private String clientVersionForTracing;
    private String applicationNameForTracing;
    private static final String DEFAULT_DEVICE_AUTH_TENANT = "organizations";

    private ConnectionStringBuilder(String clusterUrl) {
        this.clusterUrl = clusterUrl;
        this.usernameHint = null;
        this.applicationClientId = null;
        this.applicationKey = null;
        this.x509Certificate = null;
        this.x509CertificateChain = null;
        this.privateKey = null;
        this.aadAuthorityId = null;
        this.accessToken = null;
        this.tokenProvider = null;
        this.managedIdentityClientId = null;
        this.useDeviceCodeAuth = false;
        this.useManagedIdentityAuth = false;
        this.useUserPromptAuth = false;
        this.userNameForTracing = null;
        this.clientVersionForTracing = null;
        this.applicationNameForTracing = null;
    }

    public ConnectionStringBuilder(ConnectionStringBuilder other) {
        this.clusterUrl = other.clusterUrl;
        this.usernameHint = other.usernameHint;
        this.applicationClientId = other.applicationClientId;
        this.applicationKey = other.applicationKey;
        this.x509Certificate = other.x509Certificate;
        this.x509CertificateChain = other.x509CertificateChain;
        this.privateKey = other.privateKey;
        this.aadAuthorityId = other.aadAuthorityId;
        this.accessToken = other.accessToken;
        this.tokenProvider = other.tokenProvider;
        this.managedIdentityClientId = other.managedIdentityClientId;
        this.useDeviceCodeAuth = other.useDeviceCodeAuth;
        this.useManagedIdentityAuth = other.useManagedIdentityAuth;
        this.useUserPromptAuth = other.useUserPromptAuth;
        this.userNameForTracing = other.userNameForTracing;
        this.clientVersionForTracing = other.clientVersionForTracing;
        this.applicationNameForTracing = other.applicationNameForTracing;
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    public void setClusterUrl(String clusterUrl) {
        this.clusterUrl = clusterUrl;
    }

    String getUserUsernameHint() {
        return usernameHint;
    }

    String getApplicationClientId() {
        return applicationClientId;
    }

    String getApplicationKey() {
        return applicationKey;
    }

    X509Certificate getX509Certificate() {
        return x509Certificate;
    }

    List<X509Certificate> getX509CertificateChain() {
        return x509CertificateChain;
    }

    PrivateKey getPrivateKey() {
        return privateKey;
    }

    String getAuthorityId() {
        return aadAuthorityId;
    }

    String getAccessToken() {
        return accessToken;
    }

    public Callable<String> getTokenProvider() {
        return tokenProvider;
    }

    public String getManagedIdentityClientId() {
        return managedIdentityClientId;
    }

    boolean isUseDeviceCodeAuth() {
        return useDeviceCodeAuth;
    }

    boolean isUseManagedIdentityAuth() {
        return useManagedIdentityAuth;
    }

    boolean isUseUserPromptAuth() {
        return useUserPromptAuth;
    }

    public String getUserNameForTracing() {
        return userNameForTracing;
    }

    public void setUserNameForTracing(String userNameForTracing) {
        this.userNameForTracing = userNameForTracing;
    }

    public String getClientVersionForTracing() {
        return clientVersionForTracing;
    }

    public String getApplicationNameForTracing() {
        return applicationNameForTracing;
    }

    public void setApplicationNameForTracing(String applicationNameForTracing) {
        this.applicationNameForTracing = applicationNameForTracing;
    }

    public void setClientVersionForTracing(String clientVersionForTracing) {
        this.clientVersionForTracing = clientVersionForTracing;
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String clusterUrl,
            String applicationClientId,
            String applicationKey) {
        return createWithAadApplicationCredentials(clusterUrl, applicationClientId, applicationKey, null);
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String clusterUrl,
            String applicationClientId,
            String applicationKey,
            String authorityId) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationClientId)) {
            throw new IllegalArgumentException("applicationClientId cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationKey)) {
            throw new IllegalArgumentException("applicationKey cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.applicationClientId = applicationClientId;
        csb.applicationKey = applicationKey;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithUserPrompt(String clusterUrl) {
        return createWithUserPrompt(clusterUrl, DEFAULT_DEVICE_AUTH_TENANT, null);
    }

    public static ConnectionStringBuilder createWithUserPrompt(String clusterUrl, String usernameHint) {
        return createWithUserPrompt(clusterUrl, DEFAULT_DEVICE_AUTH_TENANT, usernameHint);
    }

    public static ConnectionStringBuilder createWithUserPrompt(String clusterUrl, String authorityId, String usernameHint) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.aadAuthorityId = authorityId;
        csb.usernameHint = usernameHint;
        csb.useUserPromptAuth = true;
        return csb;
    }

    public static ConnectionStringBuilder createWithDeviceCode(String clusterUrl) {
        return createWithDeviceCode(clusterUrl, DEFAULT_DEVICE_AUTH_TENANT);
    }

    public static ConnectionStringBuilder createWithDeviceCode(String clusterUrl, String authorityId) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.aadAuthorityId = authorityId;
        csb.useDeviceCodeAuth = true;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificate(String clusterUrl,
            String applicationClientId,
            X509Certificate x509Certificate,
            PrivateKey privateKey) {
        return createWithAadApplicationCertificate(clusterUrl, applicationClientId, x509Certificate, privateKey, null);
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificate(String clusterUrl,
            String applicationClientId,
            X509Certificate x509Certificate,
            PrivateKey privateKey,
            String authorityId) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationClientId)) {
            throw new IllegalArgumentException("applicationClientId cannot be null or empty");
        }
        if (x509Certificate == null) {
            throw new IllegalArgumentException("certificate cannot be null");
        }
        if (privateKey == null) {
            throw new IllegalArgumentException("privateKey cannot be null");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.applicationClientId = applicationClientId;
        csb.x509Certificate = x509Certificate;
        csb.privateKey = privateKey;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificateSubjectNameIssuer(String clusterUrl,
            String applicationClientId,
            List<X509Certificate> x509CertificateChain,
            PrivateKey privateKey) {
        return createWithAadApplicationCertificateSubjectNameIssuer(clusterUrl, applicationClientId, x509CertificateChain, privateKey, null);
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificateSubjectNameIssuer(String clusterUrl,
            String applicationClientId,
            List<X509Certificate> x509CertificateChain,
            PrivateKey privateKey,
            String authorityId) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationClientId)) {
            throw new IllegalArgumentException("applicationClientId cannot be null or empty");
        }
        if (x509CertificateChain == null || x509CertificateChain.isEmpty()) {
            throw new IllegalArgumentException("public certificate chain cannot be null or empty");
        }
        if (privateKey == null) {
            throw new IllegalArgumentException("privateKey cannot be null");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.applicationClientId = applicationClientId;
        csb.x509CertificateChain = x509CertificateChain;
        csb.privateKey = privateKey;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadAccessTokenAuthentication(String clusterUrl, String token) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }
        if (StringUtils.isEmpty(token)) {
            throw new IllegalArgumentException("token cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.accessToken = token;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadTokenProviderAuthentication(String clusterUrl, Callable<String> tokenProviderCallable) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        if (tokenProviderCallable == null) {
            throw new IllegalArgumentException("tokenProviderCallback cannot be null");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.tokenProvider = tokenProviderCallable;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadManagedIdentity(String clusterUrl) {
        return createWithAadManagedIdentity(clusterUrl, null);
    }

    public static ConnectionStringBuilder createWithAadManagedIdentity(String clusterUrl, String managedIdentityClientId) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(clusterUrl);
        csb.managedIdentityClientId = managedIdentityClientId;
        csb.useManagedIdentityAuth = true;
        return csb;
    }
}
