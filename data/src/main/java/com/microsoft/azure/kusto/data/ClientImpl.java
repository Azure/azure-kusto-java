// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.auth.TokenProviderBase;
import com.microsoft.azure.kusto.data.auth.TokenProviderFactory;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceError;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.utils.URIBuilder;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClientImpl implements Client, StreamingClient {
    private static final String ADMIN_COMMANDS_PREFIX = ".";
    private static final String MGMT_ENDPOINT_VERSION = "v1";
    private static final String QUERY_ENDPOINT_VERSION = "v2";
    private static final String STREAMING_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";
    private static final Long COMMAND_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);
    private static final Long QUERY_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(4);
    private static final Long STREAMING_INGEST_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);
    private static final int CLIENT_SERVER_DELTA_IN_MILLISECS = (int) TimeUnit.SECONDS.toMillis(30);
    public static final String FEDERATED_SECURITY_POSTFIX = ";fed=true";
    private final TokenProviderBase aadAuthenticationHelper;
    private final String clusterUrl;
    private String clientVersionForTracing;
    private final String applicationNameForTracing;

    public ClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        String url = csb.getClusterUrl();
        URI clusterUri = new URI(url);
        String host = clusterUri.getHost();
        String auth = clusterUri.getAuthority().toLowerCase();
        if (host == null && auth.endsWith(FEDERATED_SECURITY_POSTFIX)) {
            url = new URIBuilder().setScheme(clusterUri.getScheme()).setHost(auth.substring(0, clusterUri.getAuthority().indexOf(FEDERATED_SECURITY_POSTFIX))).toString();
            csb.setClusterUrl(url);
        }

        clusterUrl = url;
        aadAuthenticationHelper = TokenProviderFactory.createTokenProvider(csb);
        clientVersionForTracing = "Kusto.Java.Client";
        String version = Utils.getPackageVersion();
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
        String response = executeToJsonResult(database, command, properties);

        String clusterEndpoint = determineClusterEndpoint(command);
        try {
            return new KustoOperationResult(response, clusterEndpoint.endsWith("v2/rest/query") ? "v2" : "v1");
        } catch (KustoServiceError e) {
            throw new DataServiceException(clusterEndpoint,
                    "Error parsing json response as KustoOperationResult:" + e.getMessage(), e, e.isPermanent());
        } catch (Exception e){
            throw new DataClientException(clusterEndpoint, e.getMessage(), e);
        }
    }

    @Override
    public String executeToJsonResult(String command) throws DataServiceException, DataClientException {
        return executeToJsonResult(DEFAULT_DATABASE_NAME, command);
    }

    @Override
    public String executeToJsonResult(String database, String command) throws DataServiceException, DataClientException {
        return executeToJsonResult(database, command, null);
    }

    @Override
    public String executeToJsonResult(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException {
        // Argument validation
        if (StringUtils.isEmpty(database)) {
            throw new IllegalArgumentException("Database is empty");
        }
        if (StringUtils.isEmpty(command)) {
            throw new IllegalArgumentException("Command is empty");
        }
        command = command.trim();

        String clusterEndpoint = determineClusterEndpoint(command);
        long timeoutMs = determineTimeout(properties, command);

        Map<String, String> headers = generateIngestAndCommandHeaders(properties, "KJC.execute");
        addCommandHeaders(headers);
        String jsonPayload = generateCommandPayload(database, command, properties, clusterEndpoint);

        return Utils.post(clusterEndpoint, jsonPayload, null, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers, false);
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
        Map<String, String> headers = generateIngestAndCommandHeaders(properties, "KJC.executeStreamingIngest");

        Long timeoutMs = null;
        if (properties != null) {
            timeoutMs = properties.getTimeoutInMilliSec();
            Iterator<Map.Entry<String, Object>> iterator = properties.getOptions();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> pair = iterator.next();
                headers.put(pair.getKey(), pair.getValue().toString());
            }
        }

        headers.put(HttpHeaders.CONTENT_ENCODING, "gzip");

        if (timeoutMs == null) {
            timeoutMs = STREAMING_INGEST_TIMEOUT_IN_MILLISECS;
        }
        String response = Utils.post(clusterEndpoint, null, stream, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers, leaveOpen);
        try {
            return new KustoOperationResult(response, "v1");
        } catch (KustoServiceError e) {
            throw new DataClientException(clusterEndpoint, "Error converting json response to KustoOperationResult:" + e.getMessage(), e);
        }
    }

    @Override
    public InputStream executeStreamingQuery(String command) throws DataServiceException, DataClientException {
        return executeStreamingQuery(DEFAULT_DATABASE_NAME, command);
    }

    @Override
    public InputStream executeStreamingQuery(String database, String command) throws DataServiceException, DataClientException {
        return executeStreamingQuery(database, command, null);
    }

    @Override
    public InputStream executeStreamingQuery(String database, String command, ClientRequestProperties properties) throws DataServiceException, DataClientException {
        if (StringUtils.isEmpty(database)) {
            throw new IllegalArgumentException("Database is empty");
        }
        if (StringUtils.isEmpty(command)) {
            throw new IllegalArgumentException("Command is empty");
        }
        command = command.trim();

        String clusterEndpoint = determineClusterEndpoint(command);
        long timeoutMs = determineTimeout(properties, command);

        Map<String, String> headers = generateIngestAndCommandHeaders(properties, "KJC.executeStreaming");
        addCommandHeaders(headers);
        String jsonPayload = generateCommandPayload(database, command, properties, clusterEndpoint);

        return Utils.postToStreamingOutput(clusterEndpoint, jsonPayload, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers);
    }

    private String determineClusterEndpoint(String command) {
        String clusterEndpoint;
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            clusterEndpoint = String.format("%s/%s/rest/mgmt", clusterUrl, MGMT_ENDPOINT_VERSION);
        } else {
            clusterEndpoint = String.format("%s/%s/rest/query", clusterUrl, QUERY_ENDPOINT_VERSION);
        }
        return clusterEndpoint;
    }

    private long determineTimeout(ClientRequestProperties properties, String command) {
        Long timeoutMs = properties == null ? null : properties.getTimeoutInMilliSec();
        if (timeoutMs == null) {
            if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
                timeoutMs = COMMAND_TIMEOUT_IN_MILLISECS;
            } else {
                timeoutMs = QUERY_TIMEOUT_IN_MILLISECS;
            }
        }
        return timeoutMs;
    }

    private Map<String, String> generateIngestAndCommandHeaders(ClientRequestProperties properties, String clientRequestIdPrefix) throws DataServiceException, DataClientException {
        Map<String, String> headers = new HashMap<>();
        headers.put("x-ms-client-version", clientVersionForTracing);
        if (applicationNameForTracing != null) {
            headers.put("x-ms-app", applicationNameForTracing);
        }
        headers.put(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", aadAuthenticationHelper.acquireAccessToken()));

        String clientRequestId = null;
        if (properties != null) {
            clientRequestId = properties.getClientRequestId();
        }
        headers.put("x-ms-client-request-id", clientRequestId == null ? String.format("%s;%s", clientRequestIdPrefix, java.util.UUID.randomUUID()) : clientRequestId);
        return headers;
    }

    private String generateCommandPayload(String database, String command, ClientRequestProperties properties, String clusterEndpoint) throws DataClientException {
        String jsonPayload;
        try {
            JSONObject json = new JSONObject()
                    .put("db", database)
                    .put("csl", command);

            if (properties != null) {
                json.put("properties", properties.toString());
            }

            jsonPayload = json.toString();
        } catch (JSONException e) {
            throw new DataClientException(clusterEndpoint, String.format(clusterEndpoint, "Error executing command '%s' in database '%s'. Setting up request payload failed.", command, database), e);
        }

        return jsonPayload;
    }

    private void addCommandHeaders(Map<String, String> headers) {
        headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
        headers.put("Fed", "True");
    }
}