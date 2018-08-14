package com.microsoft.azure.kusto.data;

import org.json.JSONObject;

public class KustoClient {

    private final String adminCommandsPrefix = ".";
    private final String apiVersion = "v1";
    private final String defaultDatabaseName = "NetDefaultDb";

    private AadAuthenticationHelper aadAuthenticationHelper;
    private String clusterUrl;

    public KustoClient(KustoConnectionStringBuilder kcsb) {
        clusterUrl = kcsb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(kcsb);
    }

    public KustoResults execute(String command) throws Exception {
        return execute(defaultDatabaseName, command);
    }

    public KustoResults execute(String database, String command) throws Exception {
        String clusterEndpoint;
        if (command.startsWith(adminCommandsPrefix)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, apiVersion);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, apiVersion);
        }
        return execute(database, command, clusterEndpoint);
    }

    private KustoResults execute(String database, String command, String clusterEndpoint) throws Exception {
        String aadAccessToken = aadAuthenticationHelper.acquireAccessToken();

        String jsonString = new JSONObject()
                .put("db", database)
                .put("csl", command).toString();

        return Utils.post(clusterEndpoint, aadAccessToken, jsonString);
    }
}