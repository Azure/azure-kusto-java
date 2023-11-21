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
import com.microsoft.azure.kusto.data.http.HttpClientFactory;
import com.microsoft.azure.kusto.data.http.UncloseableStream;
import com.microsoft.azure.kusto.data.http.HttpPostUtils;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.ParseException;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jetbrains.annotations.NotNull;

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
    public static final String CLIENT_VERSION_HEADER = "x-ms-client-version";
    public static final String APP_HEADER = "x-ms-app";
    public static final String USER_HEADER = "x-ms-user";
    public static final String FEDERATED_SECURITY_SUFFIX = ";fed=true";
    public static final String JAVA_INGEST_ACTIVITY_TYPE_PREFIX = "DN.JavaClient.Execute";
    private final TokenProviderBase aadAuthenticationHelper;
    private final String clusterUrl;
    private final ClientDetails clientDetails;
    private final CloseableHttpClient httpClient;
    private final boolean leaveHttpClientOpen;
    private boolean endpointValidated = false;

    private final ObjectMapper objectMapper = Utils.getObjectMapper();

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
        clientDetails = new ClientDetails(csb.getApplicationNameForTracing(), csb.getUserNameForTracing(), csb.getClientVersionForTracing());
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
        return execute(database, command, properties, determineCommandType(command));
    }

    private KustoOperationResult execute(String database, String command, ClientRequestProperties properties, CommandType commandType)
            throws DataServiceException, DataClientException {
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<KustoOperationResult, DataServiceException, DataClientException>) () -> executeImpl(database, command, properties,
                        commandType),
                commandType.getActivityTypeSuffix().concat(".execute"),
                updateAndGetExecuteTracingAttributes(database, properties));
    }

    @Override
    public KustoOperationResult executeQuery(String command) throws DataServiceException, DataClientException {
        return executeQuery(DEFAULT_DATABASE_NAME, command);
    }

    @Override
    public KustoOperationResult executeQuery(String database, String command) throws DataServiceException, DataClientException {
        return executeQuery(database, command, null);
    }

    @Override
    public KustoOperationResult executeQuery(String database, String command, ClientRequestProperties properties)
            throws DataServiceException, DataClientException {
        return execute(database, command, properties, CommandType.QUERY);
    }

    @Override
    public KustoOperationResult executeMgmt(String command) throws DataServiceException, DataClientException {
        return executeMgmt(DEFAULT_DATABASE_NAME, command);
    }

    @Override
    public KustoOperationResult executeMgmt(String database, String command) throws DataServiceException, DataClientException {
        return executeMgmt(database, command, null);
    }

    @Override
    public KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties)
            throws DataServiceException, DataClientException {
        return execute(database, command, properties, CommandType.ADMIN_COMMAND);
    }

    private Map<String, String> updateAndGetExecuteTracingAttributes(String database, TraceableAttributes traceableAttributes) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("cluster", clusterUrl);
        attributes.put("database", database);
        if (traceableAttributes != null) {
            attributes.putAll(traceableAttributes.getTracingAttributes());
        }
        return attributes;
    }

    @NotNull
    private KustoOperationResult executeImpl(String database, String command, ClientRequestProperties properties, CommandType commandType)
            throws DataServiceException, DataClientException {
        String response = executeToJsonResult(database, command, properties);
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

        try {
            headers = generateIngestAndCommandHeaders(properties, "KJC.execute",
                    commandType.getActivityTypeSuffix());
            validateEndpoint();
        } catch (KustoClientInvalidConnectionStringException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }
        addCommandHeaders(headers);
        String jsonPayload = generateCommandPayload(database, command, properties);
        StringEntity requestEntity = new StringEntity(jsonPayload, ContentType.APPLICATION_JSON);
        // trace execution
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<String, DataServiceException, DataClientException>) () -> HttpPostUtils.post(httpClient, clusterEndpoint, requestEntity,
                        timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers),
                commandType.getActivityTypeSuffix().concat(".executeToJsonResult"));
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

        String clusterEndpoint = buildClusterEndpoint(database, table, streamFormat, mappingName);
        return executeStreamingIngestImpl(clusterEndpoint, stream, null, properties, leaveOpen);
    }

    @Override
    public KustoOperationResult executeStreamingIngestFromBlob(String database, String table, String blobUrl, ClientRequestProperties properties,
            String dataFormat, String mappingName)
            throws DataServiceException, DataClientException {
        if (blobUrl == null) {
            throw new IllegalArgumentException("The provided blobUrl is null.");
        }

        String clusterEndpoint = buildClusterEndpoint(database, table, dataFormat, mappingName)
                .concat("&sourceKind=uri");
        return executeStreamingIngestImpl(clusterEndpoint, null, blobUrl, properties, false);
    }

    private KustoOperationResult executeStreamingIngestImpl(String clusterEndpoint, InputStream stream, String blobUrl, ClientRequestProperties properties,
            boolean leaveOpen)
            throws DataServiceException, DataClientException {
        boolean isStreamSource = stream != null;
        Map<String, String> headers = generateIngestAndCommandHeaders(properties,
                "KJC.executeStreamingIngest" + (isStreamSource ? "" : "FromBlob"),
                CommandType.STREAMING_INGEST.getActivityTypeSuffix());
        if (isStreamSource) {
            headers.put(HttpHeaders.CONTENT_ENCODING, "gzip");
        }

        Long timeoutMs = populateHeadersAndGetTimeout(properties, headers);
        try (InputStream ignored = (isStreamSource && !leaveOpen) ? stream : null) {
            validateEndpoint();

            // We use UncloseableStream to prevent HttpClient From closing it
            AbstractHttpEntity entity = isStreamSource ? new InputStreamEntity(new UncloseableStream(stream))
                    : new StringEntity(new IngestionSourceStorage(blobUrl).toString(), ContentType.APPLICATION_JSON);
            String response;
            // trace executeStreamingIngest
            response = MonitoredActivity.invoke(
                    (SupplierTwoExceptions<String, DataServiceException, DataClientException>) () -> HttpPostUtils.post(httpClient, clusterEndpoint, entity,
                            timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers),
                    "ClientImpl.executeStreamingIngest");
            return new KustoOperationResult(response, "v1");
        } catch (KustoServiceQueryError e) {
            throw new DataClientException(clusterEndpoint, "Error converting json response to KustoOperationResult:" + e.getMessage(), e);
        } catch (KustoClientInvalidConnectionStringException | IOException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }
    }

    private Long populateHeadersAndGetTimeout(ClientRequestProperties properties, Map<String, String> headers) throws DataClientException {
        Long timeoutMs = null;
        if (properties != null) {
            timeoutMs = determineTimeout(properties, CommandType.STREAMING_INGEST, clusterUrl);
            Iterator<Map.Entry<String, Object>> iterator = properties.getOptions();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> pair = iterator.next();
                headers.put(pair.getKey(), pair.getValue().toString());
            }
        }
        if (timeoutMs == null) {
            timeoutMs = STREAMING_INGEST_TIMEOUT_IN_MILLISECS;
        }

        return timeoutMs;
    }

    private String buildClusterEndpoint(String database, String table, String format, String mappingName) {
        if (StringUtils.isBlank(database)) {
            throw new IllegalArgumentException("Parameter database is empty.");
        }
        if (StringUtils.isBlank(table)) {
            throw new IllegalArgumentException("Parameter table is empty.");
        }
        if (StringUtils.isBlank(format)) {
            throw new IllegalArgumentException("Parameter format is empty.");
        }
        String clusterEndpoint = String.format(CommandType.STREAMING_INGEST.getEndpoint(), clusterUrl, database, table, format);

        if (!StringUtils.isEmpty(mappingName)) {
            clusterEndpoint = clusterEndpoint.concat(String.format("&mappingName=%s", mappingName));
        }
        return clusterEndpoint;
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
        String jsonPayload = generateCommandPayload(database, command, properties);

        try {
            validateEndpoint();
        } catch (KustoClientInvalidConnectionStringException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }
        // trace httpCall
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<InputStream, DataServiceException, DataClientException>) () -> HttpPostUtils.postToStreamingOutput(httpClient,
                        clusterEndpoint,
                        jsonPayload, timeoutMs + CLIENT_SERVER_DELTA_IN_MILLISECS, headers),
                "ClientImpl.executeStreamingQuery", updateAndGetExecuteTracingAttributes(database, properties));
    }

    private long determineTimeout(ClientRequestProperties properties, CommandType commandType, String clusterUrl) throws DataClientException {
        Long timeoutMs;
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

        // replace non-ascii characters in header values with '?'
        headers.replaceAll((_i, v) -> v == null ? null : v.replaceAll("[^\\x00-\\x7F]", "?"));
        return headers;
    }

    Map<String, String> extractTracingHeaders(ClientRequestProperties properties) {
        Map<String, String> headers = new HashMap<>();

        String version = clientDetails.getClientVersionForTracing();
        if (StringUtils.isNotBlank(version)) {
            headers.put(CLIENT_VERSION_HEADER, version);
        }

        String app = (properties == null || properties.getApplication() == null) ? clientDetails.getApplicationForTracing() : properties.getApplication();
        if (StringUtils.isNotBlank(app)) {
            headers.put(APP_HEADER, app);
        }

        String user = (properties == null || properties.getUser() == null) ? clientDetails.getUserNameForTracing() : properties.getUser();
        if (StringUtils.isNotBlank(user)) {
            headers.put(USER_HEADER, user);
        }

        return headers;
    }

    private String generateCommandPayload(String database, String command, ClientRequestProperties properties) {

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
