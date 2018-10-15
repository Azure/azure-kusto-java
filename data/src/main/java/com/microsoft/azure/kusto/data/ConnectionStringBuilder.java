package com.microsoft.azure.kusto.data;

public class ConnectionStringBuilder {

    private String clusterUri;
    private String username;
    private String password;
    private String applicationClientId;
    private String applicationKey;
    private String aadAuthorityId; // AAD tenant Id (GUID)

    String getClusterUrl() { return clusterUri; }
    String getUserUsername() { return username; }
    String getUserPassword() { return password; }
    String getApplicationClientId() { return applicationClientId; }
    String getApplicationKey() { return applicationKey; }
    String getAuthorityId() { return aadAuthorityId; }

    private ConnectionStringBuilder(String resourceUri)
    {
        clusterUri = resourceUri;
        username = null;
        password = null;
        applicationClientId = null;
        applicationKey = null;
        aadAuthorityId = null;
    }

    private static ConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                        String username,
                                                                        String password,
                                                                        String authorityId)
    {
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

    public static ConnectionStringBuilder createWithDeviceCredentials(String resourceUri){
        return new ConnectionStringBuilder(resourceUri);
    }
}
