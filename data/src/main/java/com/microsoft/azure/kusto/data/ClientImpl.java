// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ClientImpl implements Client, StreamingClient {

    private static final String ADMIN_COMMANDS_PREFIX = ".";
    private static final String MGMT_ENDPOINT_VERSION = "v1";
    private static final String QUERY_ENDPOINT_VERSION = "v2";
    private static final String STREAMING_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";
    private static final Long COMMAND_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10) + TimeUnit.SECONDS.toMillis(30);
    private static final Long QUERY_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(4) + TimeUnit.SECONDS.toMillis(30);
    private static final Long STREAMING_INGEST_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);
    private static final int CLIENT_SERVER_DELTA_IN_MILLISECS = (int) TimeUnit.SECONDS.toMillis(30);

    private final AadAuthenticationHelper aadAuthenticationHelper;
    private final String clusterUrl;
    private String clientVersionForTracing;
    private final String applicationNameForTracing;

    public ClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        clusterUrl = csb.getClusterUrl();
        aadAuthenticationHelper = new AadAuthenticationHelper(csb);
        clientVersionForTracing = "Kusto.Java.Client";
        String version = Utils.GetPackageVersion();
        if (StringUtils.isNotBlank(version)) {
            clientVersionForTracing += ":" + version;
        }
        if (StringUtils.isNotBlank(csb.getClientVersionForTracing())) {
            clientVersionForTracing += "[" + csb.getClientVersionForTracing() + "]";
        }
        applicationNameForTracing = csb.getApplicationNameForTracing();
    }

    @Override
    public KustoOperationResult execute(String command) throws DataServiceException, DataClientException {
        return execute(DEFAULT_DATABASE_NAME, command);
    }

    @Override
    public KustoOperationResult execute(String database, String command) throws DataServiceException, DataClientException {
        return execute(database, command, null);
    }

    @Override
    public KustoOperationResult execute(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException {
        // Argument validation:
        if (StringUtils.isAnyEmpty(database, command)) {
            throw new IllegalArgumentException("database or command are empty");
        }
        Long timeoutMs = properties == null ? null : properties.getTimeoutInMilliSec();

        String clusterEndpoint;
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, MGMT_ENDPOINT_VERSION);
            if (timeoutMs == null) {
                timeoutMs = COMMAND_TIMEOUT_IN_MILLISECS;
            }
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, QUERY_ENDPOINT_VERSION);
            if (timeoutMs == null) {
                timeoutMs = QUERY_TIMEOUT_IN_MILLISECS;
            }
        }

        String clientRequestId = null;
        String jsonString;
        try {
            JSONObject json = new JSONObject()
                    .put("db", database)
                    .put("csl", command);

            if (properties != null) {
                json.put("properties", properties.toString());
                clientRequestId = properties.getClientRequestId();
            }

            jsonString = json.toString();
        } catch (JSONException e) {
            throw new DataClientException(clusterEndpoint, String.format(clusterEndpoint, "Error executing command '%s' in database '%s'. Setting up request payload failed.", command, database), e);
        }

        HashMap<String, String> headers = initHeaders();
        headers.put("Content-Type", "application/json");
        headers.put("x-ms-client-request-id", clientRequestId == null ? String.format("KJC.execute;%s", java.util.UUID.randomUUID()) : clientRequestId);
        headers.put("Fed", "True");

        return Utils.post(clusterEndpoint, jsonString, null, timeoutMs.intValue() + CLIENT_SERVER_DELTA_IN_MILLISECS, headers, false);
    }

    @Override
    public KustoOperationResult executeStreamingIngest(String database, String table, InputStream stream, ClientRequestProperties properties, String streamFormat, String mappingName, boolean leaveOpen) throws DataServiceException, DataClientException {
        if (stream == null) {
            throw new IllegalArgumentException("The provided stream is null.");
        }
        if (StringUtils.isBlank(database)) {
            throw new IllegalArgumentException("Parameter database is empty.");
        }
        if (StringUtils.isBlank(table)) {
            throw new IllegalArgumentException("Parameter table is empty.");
        }
        if (StringUtils.isBlank(streamFormat)) {
            throw new IllegalArgumentException("Parameter streamFormat is empty.");
        }
        String clusterEndpoint = String.format("%s/%s/rest/ingest/%s/%s?streamFormat=%s", clusterUrl, STREAMING_VERSION, database, table, streamFormat);

        if (!StringUtils.isEmpty(mappingName)) {
            clusterEndpoint = clusterEndpoint.concat(String.format("&mappingName=%s", mappingName));
        }
        HashMap<String, String> headers = initHeaders();
        String clientRequestId = null;

        Long timeoutMs = null;
        if (properties != null) {
            timeoutMs = properties.getTimeoutInMilliSec();
            clientRequestId = properties.getClientRequestId();
            Iterator<HashMap.Entry<String, Object>> iterator = properties.getOptions();
            while (iterator.hasNext()) {
                HashMap.Entry<String, Object> pair = iterator.next();
                headers.put(pair.getKey(), pair.getValue().toString());
            }
        }

        headers.put("x-ms-client-request-id", clientRequestId == null ? String.format("KJC.executeStreamingIngest;%s", java.util.UUID.randomUUID()) : clientRequestId);
        headers.put("Content-Encoding", "gzip");

        if (timeoutMs == null) {
            timeoutMs = STREAMING_INGEST_TIMEOUT_IN_MILLISECS;
        }
        return Utils.post(clusterEndpoint, null, stream, timeoutMs.intValue()  + CLIENT_SERVER_DELTA_IN_MILLISECS, headers, leaveOpen);
    }

    private HashMap<String, String> initHeaders() throws DataServiceException, DataClientException {
        HashMap<String, String> headers = new HashMap<>();
        headers.put("x-ms-client-version", clientVersionForTracing);
        if (applicationNameForTracing != null) {
            headers.put("x-ms-app", applicationNameForTracing);
        }
        headers.put("Authorization", String.format("Bearer %s", aadAuthenticationHelper.acquireAccessToken()));
        return headers;
    }
}