package com.microsoft.azure.kusto.data;

import org.json.JSONObject;

public class ClientImpl implements Client {

    private final String adminCommandsPrefix = ".";
    private final String apiVersion = "v1";
    private final String defaultDatabaseName = "NetDefaultDb";

    private AadAuthenticationHelper aadAuthenticationHelper;
    private String clusterUrl;

    public ClientImpl(ConnectionStringBuilder dcsb) {
        clusterUrl = dcsb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(dcsb);
    }

    public Results execute(String command) throws Exception {
        return execute(defaultDatabaseName, command);
    }

    public Results execute(String database, String command) throws Exception {
        String clusterEndpoint;
        if (command.startsWith(adminCommandsPrefix)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, apiVersion);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, apiVersion);
        }
        return execute(database, command, clusterEndpoint);
    }

    private Results execute(String database, String command, String clusterEndpoint) throws Exception {
        String aadAccessToken = aadAuthenticationHelper.acquireAccessToken();

        String jsonString = new JSONObject()
                .put("db", database)
                .put("csl", command).toString();

        return Utils.post(clusterEndpoint, aadAccessToken, jsonString);
    }
}