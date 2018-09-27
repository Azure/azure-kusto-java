package com.microsoft.azure.kusto.data;

import org.json.JSONObject;

public class ClientImpl implements Client {

    private static final String ADMIN_COMMANDS_PREFIX = ".";
    private static final String API_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";

    private AadAuthenticationHelper aadAuthenticationHelper;
    private String clusterUrl;

    public ClientImpl(ConnectionStringBuilder dcsb) {
        clusterUrl = dcsb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(dcsb);
    }

    public Results execute(String command) throws Exception {
        return execute(DEFAULT_DATABASE_NAME, command);
    }

    public Results execute(String database, String command) throws Exception {
        String clusterEndpoint;
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, API_VERSION);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, API_VERSION);
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