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
        ConnectionStringBuilder dcsb = new ConnectionStringBuilder(resourceUri);
        dcsb.username = username;
        dcsb.password = password;
        dcsb.aadAuthorityId = authorityId;
        return dcsb;
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
        ConnectionStringBuilder dcsb = new ConnectionStringBuilder(resourceUri);
        dcsb.applicationClientId = applicationClientId;
        dcsb.applicationKey = applicationKey;
        dcsb.aadAuthorityId = authorityId;
        return dcsb;
    }

    public static ConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                              String applicationClientId,
                                                                              String applicationKey)
    {
        return createWithAadApplicationCredentials(resourceUri, applicationClientId, applicationKey, null);
    }
}
