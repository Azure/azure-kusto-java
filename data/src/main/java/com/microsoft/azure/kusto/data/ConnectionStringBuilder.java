package com.microsoft.azure.kusto.data;

import org.apache.commons.lang3.StringUtils;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

public class ConnectionStringBuilder {

    private String clusterUri;
    private String username;
    private String password;
    private String applicationClientId;
    private String applicationKey;
    private X509Certificate x509Certificate;
    private PrivateKey privateKey;
    private String aadAuthorityId; // AAD tenant Id (GUID)
    private String clientVersionForTracing;

    String getClusterUrl() { return clusterUri; }
    String getUserUsername() { return username; }
    String getUserPassword() { return password; }
    String getApplicationClientId() { return applicationClientId; }
    String getApplicationKey() { return applicationKey; }
    String getAuthorityId() { return aadAuthorityId; }
    String getClientVersionForTracing() { return clientVersionForTracing; }
    X509Certificate getX509Certificate() { return x509Certificate; }
    PrivateKey getPrivateKey(){ return  privateKey; }
    private ConnectionStringBuilder(String resourceUri)
    {
        clusterUri = resourceUri;
        username = null;
        password = null;
        applicationClientId = null;
        applicationKey = null;
        aadAuthorityId = null;
        x509Certificate = null;
        privateKey = null;
    }

    public static ConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                        String username,
                                                                        String password,
                                                                        String authorityId)
    {
        if (StringUtils.isEmpty(resourceUri)){
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }
        if (StringUtils.isEmpty(username)){
            throw new IllegalArgumentException("username cannot be null or empty");
        }
        if (StringUtils.isEmpty(password)){
            throw new IllegalArgumentException("password cannot be null or empty");
        }
        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.username = username;
        csb.password = password;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                       String username,
                                                                       String password)
    {
        return createWithAadUserCredentials(resourceUri, username, password, null);
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                               String applicationClientId,
                                                                               String applicationKey,
                                                                               String authorityId)
    {

        if (StringUtils.isEmpty(resourceUri)){
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationClientId)){
            throw new IllegalArgumentException("applicationClientId cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationKey)){
            throw new IllegalArgumentException("applicationKey cannot be null or empty");
        }

        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.applicationClientId = applicationClientId;
        csb.applicationKey = applicationKey;
        csb.aadAuthorityId = authorityId;
        return csb;
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                              String applicationClientId,
                                                                              String applicationKey)
    {
        return createWithAadApplicationCredentials(resourceUri, applicationClientId, applicationKey, null);
    }

    public static ConnectionStringBuilder createWithDeviceCodeCredentials(String resourceUri){
        if (StringUtils.isEmpty(resourceUri)){
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }
        return new ConnectionStringBuilder(resourceUri);
    }

    public static ConnectionStringBuilder createWithAadApplicationCertificate(String resourceUri,
                                                                              String applicationClientId,
                                                                              X509Certificate x509Certificate,
                                                                              PrivateKey privateKey){
        if (StringUtils.isEmpty(resourceUri)){
            throw new IllegalArgumentException("resourceUri cannot be null or empty");
        }
        if (StringUtils.isEmpty(applicationClientId)){
            throw new IllegalArgumentException("applicationClientId cannot be null or empty");
        }
        if (x509Certificate == null){
            throw new IllegalArgumentException("certificate cannot be null");
        }
        if (privateKey == null){
            throw new IllegalArgumentException("privateKey cannot be null");
        }


        ConnectionStringBuilder csb = new ConnectionStringBuilder(resourceUri);
        csb.applicationClientId = applicationClientId;
        csb.x509Certificate = x509Certificate;
        csb.privateKey = privateKey;
        return csb;
    }

    public void setClientVersionForTracing(String clientVersionForTracing){
        this.clientVersionForTracing = clientVersionForTracing;
    }
}
