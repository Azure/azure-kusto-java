// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.auth.TokenProviderBase;
import com.microsoft.azure.kusto.data.auth.TokenProviderFactory;
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.ParseException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

class ClientImpl implements Client, StreamingClient {
    private static final String ADMIN_COMMANDS_PREFIX = ".";
    public static final String MGMT_ENDPOINT_VERSION = "v1";
    public static final String QUERY_ENDPOINT_VERSION = "v2";
    public static final String STREAMING_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";
    private static final Long COMMAND_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);
    private static final Long QUERY_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(4);
    private static final Long STREAMING_INGEST_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);
    private static final int CLIENT_SERVER_DELTA_IN_MILLISECS = (int) TimeUnit.SECONDS.toMillis(30);
    public static final String FEDERATED_SECURITY_SUFFIX = ";fed=true";
    public static final String JAVA_INGEST_ACTIVITY_TYPE_PREFIX = "DN.JavaClient.Execute";
    private final TokenProviderBase aadAuthenticationHelper;
    private final String clusterUrl;
    private String clientVersionForTracing;
    private final String applicationNameForTracing;
    private final String userNameForTracing;
    private final CloseableHttpClient httpClient;
    private final boolean leaveHttpClientOpen;
    private boolean endpointValidated = false;

    private ObjectMapper objectMapper = Utils.getObjectMapper();

    public ClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        this(csb, HttpClientProperties.builder().build());
    }

    public ClientImpl(ConnectionStringBuilder csb, HttpClientProperties properties) throws URISyntaxException {
        this(csb, HttpClientFactory.create(properties), false);
    }

    // Accepting a CloseableHttpClient so that we can create InputStream from response
    public ClientImpl(ConnectionStringBuilder csb, CloseableHttpClient httpClient, boolean leaveHttpClientOpen) throws URISyntaxException {
        URI clusterUrlForParsing = new URI(csb.getClusterUrl());
        String host = clusterUrlForParsing.getHost();
        Objects.requireNonNull(clusterUrlForParsing.getAuthority(), "clusterUri.authority");
        String auth = clusterUrlForParsing.getAuthority().toLowerCase();
        if (host == null) {
            host = StringUtils.removeEndIgnoreCase(auth, FEDERATED_SECURITY_SUFFIX);
        }
        URIBuilder uriBuilder = new URIBuilder().setScheme(clusterUrlForParsing.getScheme())
                .setHost(host);
        String path = clusterUrlForParsing.getPath();
        if (path != null && !path.isEmpty()) {
            path = StringUtils.removeEndIgnoreCase(path, FEDERATED_SECURITY_SUFFIX);
            path = StringUtils.removeEndIgnoreCase(path, "/");

            uriBuilder.setPath(path);
        }

        if (clusterUrlForParsing.getPort() != -1) {
            uriBuilder.setPort(clusterUrlForParsing.getPort());
        }
        csb.setClusterUrl(uriBuilder.build().toString());

        clusterUrl = csb.getClusterUrl();
        aadAuthenticationHelper = clusterUrl.toLowerCase().startsWith(CloudInfo.LOCALHOST) ? null : TokenProviderFactory.createTokenProvider(csb, httpClient);
        clientVersionForTracing = csb.getClientVersionForTracing();
        applicationNameForTracing = csb.getApplicationNameForTracing();
        userNameForTracing = csb.getUserNameForTracing();
        this.httpClient = httpClient;
        this.leaveHttpClientOpen = leaveHttpClientOpen;
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

        CommandType commandType = determineCommandType(command);
        String clusterEndpoint = String.format(commandType.getEndpoint(), clusterUrl);
        try {
            return new KustoOperationResult(response, clusterEndpoint.endsWith("v2/rest/query") ? "v2" : "v1");
        } catch (KustoServiceQueryError e) {
            throw new DataServiceException(clusterEndpoint,
                    "Error found while parsing json response as KustoOperationResult:" + e.getMessage(), e, e.isPermanent());
        } catch (Exception e) {
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
        CommandType commandType = determineCommandType(command);
        long timeoutMs = determineTimeout(properties, commandType, clusterUrl);
        // TODO save the uri once - no need to format everytime
        String clusterEndpoint = String.format(commandType.getEndpoint(), clusterUrl);

        Map<String, String> headers;
        headers = generateIngestAndCommandHeaders(properties, "KJC.execute",
                commandType.getActivityTypeSuffix());

        addCommandHeaders(headers);
        String jsonPayload = generateCommandPayload(database, command, properties, clusterEndpoint);
        try {
            validateEndpoint();
        } catch (KustoClientInvalidConnectionStringException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }

        return Utils.post(httpClient, clusterEndpoint, jsonPayload, null, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers, false);
    }

    private void validateEndpoint() throws DataServiceException, KustoClientInvalidConnectionStringException {
        if (!endpointValidated) {
            KustoTrustedEndpoints.validateTrustedEndpoint(clusterUrl,
                    CloudInfo.retrieveCloudInfoForCluster(clusterUrl).getLoginEndpoint());
            endpointValidated = true;
        }
    }

    @Override
    public KustoOperationResult executeStreamingIngest(String database, String table, InputStream stream, ClientRequestProperties properties,
            String streamFormat, String mappingName, boolean leaveOpen)
            throws DataServiceException, DataClientException {
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
        String clusterEndpoint = String.format(CommandType.STREAMING_INGEST.getEndpoint(), clusterUrl, database, table, streamFormat);

        if (!StringUtils.isEmpty(mappingName)) {
            clusterEndpoint = clusterEndpoint.concat(String.format("&mappingName=%s", mappingName));
        }
        Map<String, String> headers;
        headers = generateIngestAndCommandHeaders(properties, "KJC.executeStreamingIngest",
                CommandType.STREAMING_INGEST.getActivityTypeSuffix());

        Long timeoutMs = null;
        if (properties != null) {
            timeoutMs = determineTimeout(properties, CommandType.STREAMING_INGEST, clusterUrl);
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

        try {
            validateEndpoint();
            String response = Utils.post(httpClient, clusterEndpoint, null, stream, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers, leaveOpen);
            return new KustoOperationResult(response, "v1");
        } catch (KustoServiceQueryError e) {
            throw new DataClientException(clusterEndpoint, "Error converting json response to KustoOperationResult:" + e.getMessage(), e);
        } catch (KustoClientInvalidConnectionStringException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
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
    public InputStream executeStreamingQuery(String database, String command, ClientRequestProperties properties)
            throws DataServiceException, DataClientException {
        if (StringUtils.isEmpty(database)) {
            throw new IllegalArgumentException("Database is empty");
        }
        if (StringUtils.isEmpty(command)) {
            throw new IllegalArgumentException("Command is empty");
        }
        command = command.trim();
        CommandType commandType = determineCommandType(command);
        long timeoutMs = determineTimeout(properties, commandType, clusterUrl);
        String clusterEndpoint = String.format(commandType.getEndpoint(), clusterUrl);

        Map<String, String> headers;
        headers = generateIngestAndCommandHeaders(properties, "KJC.executeStreaming",
                commandType.getActivityTypeSuffix());

        addCommandHeaders(headers);
        String jsonPayload = generateCommandPayload(database, command, properties, clusterEndpoint);

        try {
            validateEndpoint();
        } catch (KustoClientInvalidConnectionStringException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }

        return Utils.postToStreamingOutput(httpClient, clusterEndpoint, jsonPayload, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers);
    }

    private long determineTimeout(ClientRequestProperties properties, CommandType commandType, String clusterUrl) throws DataClientException {
        Long timeoutMs = null;
        try {
            timeoutMs = properties == null ? null : properties.getTimeoutInMilliSec();
        } catch (ParseException e) {
            throw new DataClientException(clusterUrl, "Failed to parse timeout from ClientRequestProperties");
        }
        if (timeoutMs == null) {
            if (commandType == CommandType.ADMIN_COMMAND) {
                timeoutMs = COMMAND_TIMEOUT_IN_MILLISECS;
            } else {
                timeoutMs = QUERY_TIMEOUT_IN_MILLISECS;
            }
        }
        return timeoutMs;
    }

    private CommandType determineCommandType(String command) {
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            return CommandType.ADMIN_COMMAND;
        }
        return CommandType.QUERY;
    }

    private Map<String, String> generateIngestAndCommandHeaders(ClientRequestProperties properties,
            String clientRequestIdPrefix,
            String activityTypeSuffix)
            throws DataServiceException, DataClientException {

        Map<String, String> headers = extractTracingHeaders(properties);

        if (aadAuthenticationHelper != null) {
            headers.put(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", aadAuthenticationHelper.acquireAccessToken()));
        }

        String clientRequestId;
        if (properties != null && StringUtils.isNotBlank(properties.getClientRequestId())) {
            clientRequestId = properties.getClientRequestId();
        } else {
            clientRequestId = String.format("%s;%s", clientRequestIdPrefix, UUID.randomUUID());
        }

        headers.put("x-ms-client-request-id", clientRequestId);

        headers.put("Connection", "Keep-Alive");

        UUID activityId = UUID.randomUUID();
        String activityContext = String.format("%s%s/%s, ActivityId=%s, ParentId=%s, ClientRequestId=%s", JAVA_INGEST_ACTIVITY_TYPE_PREFIX, activityTypeSuffix,
                activityId, activityId, activityId, clientRequestId);
        headers.put("x-ms-activitycontext", activityContext);

        return headers;
    }

    Map<String, String> extractTracingHeaders(ClientRequestProperties properties) {
        Map<String, String> headers = new HashMap<>();

        properties = properties == null ? new ClientRequestProperties() : properties;

        String version = properties.getVersion() == null ? clientVersionForTracing : properties.getVersion();
        if (StringUtils.isNotBlank(version)) {
            headers.put("x-ms-client-version", version);
        }

        String app = properties.getApplication() == null ? applicationNameForTracing : properties.getApplication();
        if (StringUtils.isNotBlank(app)) {
            headers.put("x-ms-app", app);
        }

        String user = properties.getUser() == null ? userNameForTracing : properties.getUser();
        if (StringUtils.isNotBlank(user)) {
            headers.put("x-ms-user", user);
        }

        return headers;
    }

    private String generateCommandPayload(String database, String command, ClientRequestProperties properties, String clusterEndpoint) {

        ObjectNode json = objectMapper.createObjectNode()
                .put("db", database)
                .put("csl", command);

        if (properties != null) {
            json.put("properties", properties.toString());
        }

        return json.toString();
    }

    private void addCommandHeaders(Map<String, String> headers) {
        headers.put(HttpHeaders.CONTENT_TYPE, "application/json");
        headers.put("Fed", "True");
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    @Override
    public void close() throws IOException {
        if (!leaveHttpClientOpen) {
            httpClient.close();
        }
    }
}
