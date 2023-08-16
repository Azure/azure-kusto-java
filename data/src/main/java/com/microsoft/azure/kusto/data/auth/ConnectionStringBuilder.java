// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data.auth;

import com.microsoft.azure.kusto.data.ClientDetails;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import reactor.util.annotation.Nullable;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private String appendedClientVersionForTracing;
    private String applicationNameForTracing;
    private static final String DEFAULT_DEVICE_AUTH_TENANT = "organizations";

    private static final Map<String, ConnectionStringKeyword> stringKeywordMap = new HashMap<>();

    static {
        stringKeywordMap.put("Data Source".toLowerCase(), ConnectionStringKeyword.DataSource);
        stringKeywordMap.put("Addr".toLowerCase(), ConnectionStringKeyword.DataSource);
        stringKeywordMap.put("Address".toLowerCase(), ConnectionStringKeyword.DataSource);
        stringKeywordMap.put("Network Address".toLowerCase(), ConnectionStringKeyword.DataSource);
        stringKeywordMap.put("Server".toLowerCase(), ConnectionStringKeyword.DataSource);
        stringKeywordMap.put("Use User Prompt Auth".toLowerCase(), ConnectionStringKeyword.UseUserPromptAuth);
        stringKeywordMap.put("User Prompt Auth".toLowerCase(), ConnectionStringKeyword.UseUserPromptAuth);
        stringKeywordMap.put("Use User Prompt Authentication".toLowerCase(), ConnectionStringKeyword.UseUserPromptAuth);
        stringKeywordMap.put("User Prompt Authentication".toLowerCase(), ConnectionStringKeyword.UseUserPromptAuth);
        stringKeywordMap.put("Use Device Prompt Auth".toLowerCase(), ConnectionStringKeyword.UseDeviceCodeAuth);
        stringKeywordMap.put("Device Prompt Auth".toLowerCase(), ConnectionStringKeyword.UseDeviceCodeAuth);
        stringKeywordMap.put("Use Device Prompt Authentication".toLowerCase(), ConnectionStringKeyword.UseDeviceCodeAuth);
        stringKeywordMap.put("Device Code Authentication".toLowerCase(), ConnectionStringKeyword.UseDeviceCodeAuth);
        stringKeywordMap.put("Access Token".toLowerCase(), ConnectionStringKeyword.AccessToken);
        stringKeywordMap.put("AccessToken".toLowerCase(), ConnectionStringKeyword.AccessToken);

        stringKeywordMap.put("Username Hint".toLowerCase(), ConnectionStringKeyword.UsernameHint);
        stringKeywordMap.put("UsernameHint".toLowerCase(), ConnectionStringKeyword.UsernameHint);
        stringKeywordMap.put("Authority Id".toLowerCase(), ConnectionStringKeyword.AuthorityId);
        stringKeywordMap.put("Authority".toLowerCase(), ConnectionStringKeyword.AuthorityId);
        stringKeywordMap.put("TenantId".toLowerCase(), ConnectionStringKeyword.AuthorityId);
        stringKeywordMap.put("Tenant".toLowerCase(), ConnectionStringKeyword.AuthorityId);
        stringKeywordMap.put("tid".toLowerCase(), ConnectionStringKeyword.AuthorityId);
        stringKeywordMap.put("Application Client Id".toLowerCase(), ConnectionStringKeyword.ApplicationClientId);
        stringKeywordMap.put("AppClientId".toLowerCase(), ConnectionStringKeyword.ApplicationClientId);
        stringKeywordMap.put("Application Key".toLowerCase(), ConnectionStringKeyword.ApplicationKey);
        stringKeywordMap.put("AppKey".toLowerCase(), ConnectionStringKeyword.ApplicationKey);
        stringKeywordMap.put("Application Token".toLowerCase(), ConnectionStringKeyword.ApplicationToken);
        stringKeywordMap.put("AppToken".toLowerCase(), ConnectionStringKeyword.ApplicationToken);
        stringKeywordMap.put("User Token".toLowerCase(), ConnectionStringKeyword.UserToken);
        stringKeywordMap.put("UserToken".toLowerCase(), ConnectionStringKeyword.UserToken);
        stringKeywordMap.put("UsrToken".toLowerCase(), ConnectionStringKeyword.UserToken);
        stringKeywordMap.put("TraceAppName".toLowerCase(), ConnectionStringKeyword.ApplicationNameForTracing);
        stringKeywordMap.put("TraceUserName".toLowerCase(), ConnectionStringKeyword.UserNameForTracing);
        stringKeywordMap.put("TraceClientVersion".toLowerCase(), ConnectionStringKeyword.ClientVersionForTracing);
        stringKeywordMap.put("Data Source Uri".toLowerCase(), ConnectionStringKeyword.DataSourceUri);
        stringKeywordMap.put("DataSourceUri".toLowerCase(), ConnectionStringKeyword.DataSourceUri);
        stringKeywordMap.put("ClusterUri".toLowerCase(), ConnectionStringKeyword.DataSourceUri);
        stringKeywordMap.put("Cluster Uri".toLowerCase(), ConnectionStringKeyword.DataSourceUri);
        stringKeywordMap.put("ServerUri".toLowerCase(), ConnectionStringKeyword.DataSourceUri);
        stringKeywordMap.put("Server Uri".toLowerCase(), ConnectionStringKeyword.DataSourceUri);
    }

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
        this.appendedClientVersionForTracing = null;
        this.applicationNameForTracing = null;
    }

    private static void assignValue(String rawKey, String value, ConnectionStringBuilder csb) {
        rawKey = rawKey.trim().toLowerCase();
        ConnectionStringKeyword parsedKey = stringKeywordMap.get(rawKey);
        if (parsedKey == null) {
            throw new IllegalArgumentException("Error: unsupported key " + rawKey + " in connection string");
        }
        switch (parsedKey) {
            case DataSourceUri:
            case DataSource:
                csb.clusterUrl = value;
                break;
            case ApplicationClientId:
                csb.applicationClientId = value;
                break;
            case ApplicationKey:
                csb.applicationKey = value;
                break;
            case AuthorityId:
                csb.aadAuthorityId = value;
                break;
            case ApplicationNameForTracing:
                csb.applicationNameForTracing = value;
                break;
            case UserNameForTracing:
                csb.userNameForTracing = value;
                break;
            case UseUserPromptAuth:
                csb.useUserPromptAuth = Boolean.parseBoolean(value);
                break;
            case UseDeviceCodeAuth:
                csb.useDeviceCodeAuth = Boolean.parseBoolean(value);
                break;
            case UsernameHint:
                csb.usernameHint = value;
                break;
            case AccessToken:
                csb.accessToken = value;
                break;
            default:
                throw new IllegalArgumentException("Error: unsupported key " + rawKey + " in connection string");
        }
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
        this.appendedClientVersionForTracing = other.appendedClientVersionForTracing;
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

    // NewConnectionStringBuilder Creates new Kusto ConnectionStringBuilder.
    // Params takes kusto connection string connStr: string. Kusto connection string should be of the format:
    // https://<clusterName>.<location>.kusto.windows.net;AAD User ID="user@microsoft.com";Password=P@ssWord
    // For more information please look at:
    // https://docs.microsoft.com/azure/data-explorer/kusto/api/connection-strings/kusto
    public static ConnectionStringBuilder createWithConnectionString(String connectionString) {

        if (StringUtils.isEmpty(connectionString)) {
            throw new IllegalArgumentException("connectionString cannot be null or empty");
        }

        String[] connStrArr = connectionString.split(";");
        if (!connStrArr[0].contains("=")) {
            connStrArr[0] = "Data Source=" + connStrArr[0];
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(connStrArr[0].trim());

        for (String kvp : connStrArr) {
            kvp = kvp.trim();
            if (StringUtils.isEmpty(kvp)) {
                continue;
            }
            String[] kvpArr = kvp.split("=");
            String val = kvpArr[1].trim();
            if (!StringUtils.isEmpty(val)) {
                assignValue(kvpArr[0], val, csb);
            }
        }

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

    enum ConnectionStringKeyword {
        DataSource,
        UseUserPromptAuth,
        UsernameHint,
        UseDeviceCodeAuth,
        AccessToken,
        TokenProvider,
        ApplicationClientId,
        ApplicationKey,
        ApplicationToken,
        UserToken,
        AuthorityId,
        ApplicationNameForTracing,
        UserNameForTracing,
        ClientVersionForTracing,
        DataSourceUri
    }
}
