package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class ClientImpl implements Client {

    private static final String ADMIN_COMMANDS_PREFIX = ".";
    private static final String API_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";

    private AadAuthenticationHelper aadAuthenticationHelper;
    private String clusterUrl;

    public ClientImpl(ConnectionStringBuilder csb) {
        clusterUrl = csb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(csb);
    }

    public Results execute(String command) throws DataServiceException, DataClientException {
        return execute(DEFAULT_DATABASE_NAME, command);
    }

    public Results execute(String database, String command) throws DataServiceException, DataClientException {
        String clusterEndpoint;
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, API_VERSION);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, API_VERSION);
        }
        return execute(database, command, clusterEndpoint);
    }

    private Results execute(String database, String command, String clusterEndpoint) throws DataServiceException, DataClientException {
        // Argument validation:
        if(StringUtils.isAnyEmpty(database, command, clusterEndpoint)){
            throw new IllegalArgumentException("database, command or clusterEndpoint are empty");
        }

        String aadAccessToken = aadAuthenticationHelper.acquireAccessToken();
        String jsonString;
        try {
            jsonString = new JSONObject()
                .put("db", database)
                .put("csl", command).toString();
        } catch (JSONException e) {
            throw new DataClientException(clusterEndpoint, String.format(clusterEndpoint, "Error in executing command: %s, in database: %s", command, database), e);
        }

        return Utils.post(clusterEndpoint, aadAccessToken, jsonString);
    }
}