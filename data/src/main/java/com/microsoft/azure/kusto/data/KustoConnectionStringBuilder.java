package com.microsoft.azure.kusto.data;

public class KustoConnectionStringBuilder {

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

    private KustoConnectionStringBuilder(String resourceUri)
    {
        clusterUri = resourceUri;
        username = null;
        password = null;
        applicationClientId = null;
        applicationKey = null;
        aadAuthorityId = null;
    }

    public static KustoConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                            String username,
                                                                            String password,
                                                                            String authorityId)
    {
        KustoConnectionStringBuilder kcsb = new KustoConnectionStringBuilder(resourceUri);
        kcsb.username = username;
        kcsb.password = password;
        kcsb.aadAuthorityId = authorityId;
        return kcsb;
    }

    public static KustoConnectionStringBuilder createWithAadUserCredentials(String resourceUri,
                                                                            String username,
                                                                            String password)
    {
        return createWithAadUserCredentials(resourceUri, username, password, null);
    }

    public static KustoConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                                   String applicationClientId,
                                                                                   String applicationKey,
                                                                                   String authorityId)
    {
        KustoConnectionStringBuilder kcsb = new KustoConnectionStringBuilder(resourceUri);
        kcsb.applicationClientId = applicationClientId;
        kcsb.applicationKey = applicationKey;
        kcsb.aadAuthorityId = authorityId;
        return kcsb;
    }

    public static KustoConnectionStringBuilder createWithAadApplicationCredentials(String resourceUri,
                                                                                   String applicationClientId,
                                                                                   String applicationKey)
    {
        return createWithAadApplicationCredentials(resourceUri, applicationClientId, applicationKey, null);
    }
}
