// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import org.apache.commons.lang3.StringUtils;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;

public class ConnectionStringBuilder {

    private static final String DEFAULT_DEVICE_AUTH_TENANT = "common";
    private final String clusterUri;
    private String usernameHint;
    private String applicationClientId;
    private String applicationKey;
    private X509Certificate x509Certificate;
    private PrivateKey privateKey;
    private String aadAuthorityId; // AAD tenant Id (GUID)
    private String clientVersionForTracing;
    private String applicationNameForTracing;
    private String accessToken;
    private Callable<String> tokenProvider;

    private ConnectionStringBuilder(String resourceUri) {
        clusterUri = resourceUri;
        usernameHint = null;
        applicationClientId = null;
        applicationKey = null;
        aadAuthorityId = null;
        x509Certificate = null;
        privateKey = null;
        accessToken = null;
        tokenProvider = null;
    }

    public String getClusterUrl() {
        return clusterUri;
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

    String getAuthorityId() {
        return aadAuthorityId;
    }

    String getApplicationNameForTracing() {
        return applicationNameForTracing;
    }

    public void setApplicationNameForTracing(String applicationNameForTracing) {
        this.applicationNameForTracing = applicationNameForTracing;
    }

    String getClientVersionForTracing() {
        return clientVersionForTracing;
    }

    public void setClientVersionForTracing(String clientVersionForTracing) {
        this.clientVersionForTracing = clientVersionForTracing;
    }

    X509Certificate getX509Certificate() {
        return x509Certificate;
    }

    PrivateKey getPrivateKey() {
        return privateKey;
    }

    String getAccessToken() {
        return accessToken;
    }

    public Callable<String> getTokenProvider() {
        return tokenProvider;
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                              String applicationClientId,
                                                                              String applicationKey) {
        return createWithAadApplicationCredentials(resourceUri, applicationClientId, applicationKey, null);
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                              String applicationClientId,
                                                                              String applicationKey,
                                                                              String authorityId) {
        if (StringUtils.isEmpty(resourceUri)) {
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationClientId)) {
            throw new IllegalArgumentException("applicationClientId cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationKey)) {
            throw new IllegalArgumentException("applicationKey cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.applicationClientId = applicationClientId;
        csb.applicationKey = applicationKey;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithUserPrompt(String resourceUri) {
        return createWithUserPrompt(resourceUri, DEFAULT_DEVICE_AUTH_TENANT, null);
    }

    public static ConnectionStringBuilder createWithUserPrompt(String resourceUri, String usernameHint) {
        return createWithUserPrompt(resourceUri, DEFAULT_DEVICE_AUTH_TENANT, usernameHint);
    }

    public static ConnectionStringBuilder createWithUserPrompt(String resourceUri, String authorityId, String usernameHint) {
        if (StringUtils.isEmpty(resourceUri)) {
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.aadAuthorityId = authorityId;
        csb.usernameHint = usernameHint;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificate(String resourceUri,
                                                                              String applicationClientId,
                                                                              X509Certificate x509Certificate,
                                                                              PrivateKey privateKey) {
        return createWithAadApplicationCertificate(resourceUri, applicationClientId, x509Certificate, privateKey, null);
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificate(String resourceUri,
                                                                              String applicationClientId,
                                                                              X509Certificate x509Certificate,
                                                                              PrivateKey privateKey,
                                                                              String authorityId) {
        if (StringUtils.isEmpty(resourceUri)) {
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.applicationClientId = applicationClientId;
        csb.x509Certificate = x509Certificate;
        csb.privateKey = privateKey;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadAccessTokenAuthentication(String resourceUri, String token) {
        if (StringUtils.isEmpty(resourceUri)) {
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }
        if (StringUtils.isEmpty(token)) {
            throw new IllegalArgumentException("token cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.accessToken = token;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadTokenProviderAuthentication(String resourceUri, Callable<String> tokenProviderCallable) {
        if (StringUtils.isEmpty(resourceUri)) {
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }

        if (tokenProviderCallable == null) {
            throw new IllegalArgumentException("tokenProviderCallback cannot be null");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.tokenProvider = tokenProviderCallable;
        return csb;
    }
}