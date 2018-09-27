package com.microsoft.azure.kusto.data;

public class DataConnectionStringBuilder {

    private String clusterUri;
    private String username;
    private String password;
    private String applicationClientId;
    private String applicationKey;
    private String aadAuthorityId; // AAD tenant Id (GUID)

    public String getClusterUrl() { return clusterUri; }
    public String getUserUsername() { return username; }
    public String getUserPassword() { return password; }
    public String getApplicationClientId() { return applicationClientId; }
    public String getApplicationKey() { return applicationKey; }
    public String getAuthorityId() { return aadAuthorityId; }

    private DataConnectionStringBuilder(String resourceUri)
    {
        clusterUri = resourceUri;
        username = null;
        password = null;
        applicationClientId = null;
        applicationKey = null;
        aadAuthorityId = null;
    }

    public static DataConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                           String username,
                                                                           String password,
                                                                           String authorityId)
    {
        DataConnectionStringBuilder dcsb = new DataConnectionStringBuilder(resourceUri);
        dcsb.username = username;
        dcsb.password = password;
        dcsb.aadAuthorityId = authorityId;
        return dcsb;
    }

    public static DataConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                           String username,
                                                                           String password)
    {
        return createWithAadUserCredentials(resourceUri, username, password, null);
    }

    public static DataConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                                  String applicationClientId,
                                                                                  String applicationKey,
                                                                                  String authorityId)
    {
        DataConnectionStringBuilder dcsb = new DataConnectionStringBuilder(resourceUri);
        dcsb.applicationClientId = applicationClientId;
        dcsb.applicationKey = applicationKey;
        dcsb.aadAuthorityId = authorityId;
        return dcsb;
    }

    public static DataConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                                  String applicationClientId,
                                                                                  String applicationKey)
    {
        return createWithAadApplicationCredentials(resourceUri, applicationClientId, applicationKey, null);
    }
}
