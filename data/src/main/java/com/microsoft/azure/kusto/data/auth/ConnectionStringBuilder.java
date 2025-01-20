// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.azure.core.credential.TokenCredential;
import com.microsoft.azure.kusto.data.ClientDetails;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.*;
import java.util.concurrent.Callable;

public class ConnectionStringBuilder {
    public static final String SECRET_REPLACEMENT = "****";
    private String clusterUrl;
    private String usernameHint;
    private String applicationClientId;
    private String applicationKey;
    // Public certificate
    private String initialCatalog;
    private boolean sendX509;
    private boolean aadFederatedSecurity;
    private X509Certificate x509Certificate;
    // Chain comprised of public certificate, its CA's certificate, and the root CA's certificate
    private List<X509Certificate> x509CertificateChain;
    // PEM-encoded private key
    private PrivateKey privateKey;
    // AAD tenant Id (GUID or "microsoft.com")
    private String aadAuthorityId;
    private String accessToken;
    private Callable<String> tokenProvider;
    private Mono<String> asyncTokenProvider;
    private TokenCredential customTokenCredential;
    private String managedIdentityClientId;
    private boolean useDeviceCodeAuth;
    private boolean useManagedIdentityAuth;
    private boolean useAzureCli;
    private boolean useUserPromptAuth;
    // TODO - use this for when moving to az identity
    private boolean useCertificateAuth;
    private String userNameForTracing;
    private String appendedClientVersionForTracing;
    private String applicationNameForTracing;
    private static final String DEFAULT_DEVICE_AUTH_TENANT = "organizations";

    private static final KcsbKeywords KCSB_KEYWORDS = KcsbKeywords.getInstance();

    private ConnectionStringBuilder() {
        this.aadFederatedSecurity = false;
        this.clusterUrl = null;
        this.usernameHint = null;
        this.applicationClientId = null;
        this.applicationKey = null;
        this.x509Certificate = null;
        this.x509CertificateChain = null;
        this.privateKey = null;
        this.aadAuthorityId = null;
        this.accessToken = null;
        this.tokenProvider = null;
        this.asyncTokenProvider = null;
        this.customTokenCredential = null;
        this.managedIdentityClientId = null;
        this.useDeviceCodeAuth = false;
        this.useManagedIdentityAuth = false;
        this.useAzureCli = false;
        this.useUserPromptAuth = false;
        this.useCertificateAuth = false;
        this.userNameForTracing = null;
        this.appendedClientVersionForTracing = null;
        this.applicationNameForTracing = null;
        this.sendX509 = false;
        this.initialCatalog = null;
    }

    private void assignValue(String rawKey, String value) {
        KnownKeywords parsedKey = KCSB_KEYWORDS.get(rawKey);
        if (parsedKey == null) {
            throw new IllegalArgumentException("Error: unsupported key " + rawKey + " in connection string");
        }

        switch (parsedKey) {
            case DATA_SOURCE:
                this.clusterUrl = value;
                break;
            case INITIAL_CATALOG:
                this.initialCatalog = value;
                break;
            case FEDERATED_SECURITY: // todo - decide default for this
                this.aadFederatedSecurity = Boolean.parseBoolean(value);
                break;
            case APPLICATION_CLIENT_ID:
                this.applicationClientId = value;
                break;
            case APPLICATION_KEY:
                this.applicationKey = value;
                break;
            case AUTHORITY_ID:
                this.aadAuthorityId = value;
                break;
            case APPLICATION_CERTIFICATE_X5C:
                this.sendX509 = Boolean.parseBoolean(value);
                break;
            case APPLICATION_NAME_FOR_TRACING:
                this.applicationNameForTracing = value;
                break;
            case USER_NAME_FOR_TRACING:
                this.userNameForTracing = value;
                break;
            case USER_ID:
                this.usernameHint = value;
                this.useUserPromptAuth = true;
                break;
            case USER_TOKEN:
            case APPLICATION_TOKEN:
                this.accessToken = value;
                break;
            default:
                throw new IllegalArgumentException("Error: unsupported key " + rawKey + " in connection string");
        }
    }

    public String toString(boolean showSecrets) {
        ArrayList<Pair<KnownKeywords, String>> entries = new ArrayList<>();
        if (!StringUtils.isBlank(clusterUrl)) {
            entries.add(Pair.of(KnownKeywords.DATA_SOURCE, clusterUrl));
        }

        if (!StringUtils.isBlank(usernameHint)) {
            entries.add(Pair.of(KnownKeywords.USER_ID, usernameHint));
        }

        if (!StringUtils.isBlank(applicationClientId)) {
            entries.add(Pair.of(KnownKeywords.APPLICATION_CLIENT_ID, applicationClientId));
        }

        if (!StringUtils.isBlank(applicationKey)) {
            entries.add(Pair.of(KnownKeywords.APPLICATION_KEY, applicationKey));
        }

        if (!StringUtils.isBlank(aadAuthorityId)) {
            entries.add(Pair.of(KnownKeywords.AUTHORITY_ID, aadAuthorityId));
        }

        if (!StringUtils.isBlank(accessToken)) {
            entries.add(Pair.of(KnownKeywords.USER_TOKEN, accessToken));
        }

        if (!StringUtils.isBlank(applicationNameForTracing)) {
            entries.add(Pair.of(KnownKeywords.APPLICATION_NAME_FOR_TRACING, applicationNameForTracing));
        }

        if (!StringUtils.isBlank(userNameForTracing)) {
            entries.add(Pair.of(KnownKeywords.USER_NAME_FOR_TRACING, userNameForTracing));
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < entries.size(); i++) {
            Pair<KnownKeywords, String> entry = entries.get(i);
            sb.append(entry.getLeft().getCanonicalName())
                .append("=")
                .append((!showSecrets && entry.getLeft().isSecret()) ? SECRET_REPLACEMENT : entry.getRight());
                
            if (i < entries.size() - 1) {
                sb.append(";");
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toString(false);
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
        this.asyncTokenProvider = other.asyncTokenProvider;
        this.customTokenCredential = other.customTokenCredential;
        this.managedIdentityClientId = other.managedIdentityClientId;
        this.useAzureCli = other.useAzureCli;
        this.useDeviceCodeAuth = other.useDeviceCodeAuth;
        this.useManagedIdentityAuth = other.useManagedIdentityAuth;
        this.useUserPromptAuth = other.useUserPromptAuth;
        this.userNameForTracing = other.userNameForTracing;
        this.appendedClientVersionForTracing = other.appendedClientVersionForTracing;
        this.applicationNameForTracing = other.applicationNameForTracing;
        this.sendX509 = other.sendX509;
        this.initialCatalog = other.initialCatalog;
        this.aadFederatedSecurity = other.aadFederatedSecurity;
        this.useCertificateAuth = other.useCertificateAuth;
    }

    /**
     * Creates a ConnectionStringBuilder from a connection string. For more information please look <a href="https://docs.microsoft.com/azure/data-explorer/kusto/api/connection-strings/kusto">here</a>.
     *
     * @param connectionString The connection string should be of the format: <p>https://clusterName.location.kusto.windows.net;Fed=true;Application Client Id=****;Application Key=****</p>
     * @throws IllegalArgumentException If the connection string is invalid.
     */
    public ConnectionStringBuilder(String connectionString) {
        if (StringUtils.isBlank(connectionString)) {
            throw new IllegalArgumentException("connectionString cannot be null or empty");
        }

        String[] connStrArr = connectionString.split(";");
        if (!connStrArr[0].contains("=")) {
            connStrArr[0] = KnownKeywords.DATA_SOURCE.getCanonicalName() + "=" + connStrArr[0];
        }

        for (String kvp : connStrArr) {
            kvp = kvp.trim();
            if (StringUtils.isEmpty(kvp)) {
                continue;
            }
            String[] kvpArr = kvp.split("=");
            String val = kvpArr[1].trim();
            if (!StringUtils.isEmpty(val)) {
                assignValue(kvpArr[0], val);
            }
        }
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

    public Mono<String> getAsyncTokenProvider() {
        return asyncTokenProvider;
    }

    public TokenCredential getCustomTokenCredential() {
        return customTokenCredential;
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

    boolean isUseAzureCli() {
        return useAzureCli;
    }

    boolean isUseUserPromptAuth() {
        return useUserPromptAuth;
    }

    /**
     * Gets the username for tracing.
     * By default, it is the username of the current process as returned by the system property "user.name".
     *
     * @return The username for tracing.
     */
    public String getUserNameForTracing() {
        return userNameForTracing;
    }

    /**
     * Sets the username for tracing.
     *
     * @param userNameForTracing The username for tracing.
     */
    public void setUserNameForTracing(String userNameForTracing) {
        this.userNameForTracing = userNameForTracing;
    }

    /**
     * Gets the client version for tracing.
     * By default it is the version of the Kusto Java SDK.
     *
     * @return The client version for tracing.
     */
    public String getClientVersionForTracing() {
        return appendedClientVersionForTracing;
    }

    /**
     * @param clientVersionForTracing The client version for tracing.
     * @deprecated This field will be reserved to only the version of the Kusto Java SDK in the future. Use {@link #setApplicationNameForTracing(String)} instead.
     * Sets the client version for tracing.
     * This appends the given version to the Kusto Java SDK version.
     */
    public void setClientVersionForTracing(String clientVersionForTracing) {
        this.appendedClientVersionForTracing = clientVersionForTracing;
    }

    /**
     * Gets the application name for tracing purposes.
     * By default, it is the name of the current process as returned by the system property "sun.java.command".
     *
     * @return The application name for tracing purposes.
     */
    public String getApplicationNameForTracing() {
        return applicationNameForTracing;
    }

    /**
     * Sets the application name for tracing purposes.
     *
     * @param applicationNameForTracing The application name for tracing purposes.
     */
    public void setApplicationNameForTracing(String applicationNameForTracing) {
        this.applicationNameForTracing = applicationNameForTracing;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.applicationClientId = applicationClientId;
        csb.useCertificateAuth = true;
        csb.sendX509 = false;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.useCertificateAuth = true;
        csb.sendX509 = true;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
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

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.tokenProvider = tokenProviderCallable;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadAsyncTokenProviderAuthentication(String clusterUrl, Mono<String> tokenProviderCallable) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        if (tokenProviderCallable == null) {
            throw new IllegalArgumentException("tokenProviderCallback cannot be null");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.asyncTokenProvider = tokenProviderCallable;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadManagedIdentity(String clusterUrl) {
        return createWithAadManagedIdentity(clusterUrl, null);
    }

    public static ConnectionStringBuilder createWithAadManagedIdentity(String clusterUrl, String managedIdentityClientId) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.managedIdentityClientId = managedIdentityClientId;
        csb.useManagedIdentityAuth = true;
        return csb;
    }

    public static ConnectionStringBuilder createWithAzureCli(String clusterUrl) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.useAzureCli = true;
        return csb;
    }

    public static ConnectionStringBuilder createWithTokenCredential(@NotNull String clusterUrl, @Nullable TokenCredential tokenCredential) {
        if (StringUtils.isEmpty(clusterUrl)) {
            throw new IllegalArgumentException("clusterUrl cannot be null or empty");
        }

        if (tokenCredential == null) {
            throw new IllegalArgumentException("tokenCredential cannot be null");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder();
        csb.aadFederatedSecurity = true;
        csb.clusterUrl = clusterUrl;
        csb.customTokenCredential = tokenCredential;
        return csb;
    }

    /**
     * Sets the application name and username for Kusto connectors.
     *
     * @param name             The name of the connector/application.
     * @param version          The version of the connector/application.
     * @param appName          The app hosting the connector, or null to use the current process name.
     * @param appVersion       The version of the app hosting the connector, or null to use "[none]".
     * @param sendUser         True if the user should be sent to Kusto, otherwise "[none]" will be sent.
     * @param overrideUser     The user to send to Kusto, or null zvto use the current user.
     * @param additionalFields Additional fields to trace.
     *                         Example: "Kusto.MyConnector:{1.0.0}|App.{connector}:{0.5.3}|Kusto.MyField:{MyValue}"
     */
    public void setConnectorDetails(String name, String version, @Nullable String appName, @Nullable String appVersion, boolean sendUser,
            @Nullable String overrideUser, Pair<String, String>... additionalFields) {
        ClientDetails clientDetails = ClientDetails.fromConnectorDetails(name, version, sendUser, overrideUser, appName, appVersion, additionalFields);
        applicationNameForTracing = clientDetails.getApplicationForTracing();
        userNameForTracing = clientDetails.getUserNameForTracing();
    }

}
