// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpRequest;
import com.azure.core.util.BinaryData;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.auth.TokenProviderBase;
import com.microsoft.azure.kusto.data.auth.TokenProviderFactory;
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.exceptions.*;
import com.microsoft.azure.kusto.data.http.*;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.SupplierOneException;
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import com.microsoft.azure.kusto.data.req.KustoQuery;
import org.apache.commons.lang3.StringUtils;

import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

class ClientImpl extends BaseClient {
    private static final String ADMIN_COMMANDS_PREFIX = ".";
    public static final String MGMT_ENDPOINT_VERSION = "v1";
    public static final String QUERY_ENDPOINT_VERSION = "v2";
    public static final String STREAMING_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";

    public static final String FEDERATED_SECURITY_SUFFIX = ";fed=true";
    private final TokenProviderBase aadAuthenticationHelper;

    // Todo: Add Keep-Alive?
    // private final String keepAlive;
    private final String clusterUrl;
    private final ClientDetails clientDetails;
    private boolean endpointValidated = false;

    public ClientImpl(ConnectionStringBuilder csb) throws URISyntaxException {
        this(csb, HttpClientProperties.builder().build());
    }

    public ClientImpl(ConnectionStringBuilder csb, HttpClientProperties properties) throws URISyntaxException {
        this(csb, HttpClientFactory.create(properties));
    }

    public ClientImpl(ConnectionStringBuilder csb, HttpClient httpClient) throws URISyntaxException {
        super(httpClient);

        URI clusterUrlForParsing = new URI(csb.getClusterUrl());
        String host = clusterUrlForParsing.getHost();
        Objects.requireNonNull(clusterUrlForParsing.getAuthority(), "clusterUri must have uri authority component");
        String auth = clusterUrlForParsing.getAuthority().toLowerCase();
        if (host == null) {
            host = StringUtils.removeEndIgnoreCase(auth, FEDERATED_SECURITY_SUFFIX);
        }
        URIBuilder uriBuilder = new URIBuilder()
                .setScheme(clusterUrlForParsing.getScheme())
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
    }

    @Override
    public KustoOperationResult execute(KustoQuery kq) throws DataServiceException, DataClientException {
        return executeImpl(kq);
    }

    @Override
    public KustoOperationResult executeQuery(KustoQuery kq) throws DataServiceException, DataClientException {
        if (kq != null && kq.getCommandType() != CommandType.QUERY) {
            kq.setCommandType(CommandType.QUERY);
        }
        return executeImpl(kq);
    }

    @Override
    public KustoOperationResult executeMgmt(KustoQuery kq) throws DataServiceException, DataClientException {
        if (kq != null && kq.getCommandType() != CommandType.ADMIN_COMMAND) {
            kq.setCommandType(CommandType.ADMIN_COMMAND);
        }
        return executeImpl(kq);
    }

    @Override
    public Mono<KustoOperationResult> executeAsync(KustoQuery kq) {
        return null;
    }

    @Override
    public Mono<KustoOperationResult> executeQueryAsync(KustoQuery kq) {
        return null;
    }

    @Override
    public Mono<KustoOperationResult> executeMgmtAsync(KustoQuery kq) {
        return null;
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
        KustoQuery kq = new KustoQuery(command, database, properties, commandType);
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<KustoOperationResult, DataServiceException, DataClientException>) () -> executeImpl(kq),
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
    private KustoOperationResult executeImpl(KustoQuery kq) throws DataServiceException, DataClientException {
        String response = executeToJsonResult(kq);
        String clusterEndpoint = String.format(kq.getCommandType().getEndpoint(), clusterUrl);
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
    public String executeToJsonResult(KustoQuery kq) throws DataServiceException, DataClientException {
        if (kq == null) {
            throw new IllegalArgumentException("KustoQuery object cannot be null in order to be executed.");
        }
        if (kq.getDatabase() == null) {
            kq.setDatabase(DEFAULT_DATABASE_NAME);
        }
        // Argument validation
        if (StringUtils.isEmpty(kq.getDatabase())) {
            throw new IllegalArgumentException("Database is empty");
        }
        if (StringUtils.isEmpty(kq.getCommand())) {
            throw new IllegalArgumentException("Command is empty");
        }
        kq.setCommand(kq.getCommand().trim());
        if (kq.getCommandType() == null) {
            kq.setCommandType(determineCommandType(kq.getCommand()));
        }
        String clusterEndpoint = String.format(kq.getCommandType().getEndpoint(), clusterUrl);
        String authorization = getAuthorizationHeaderValue();

        // Validate the endpoint
        validateEndpoint();

        // Build the tracing object
        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(kq.getProperties())
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix(kq.getCommandType().getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        // Build the HTTP request
        HttpRequest request = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .createCommandPayload(kq)
                .withTracing(tracing)
                .withAuthorization(authorization)
                .build();

        // Get the response and trace the call
        return MonitoredActivity.invoke(
                (SupplierOneException<String, DataServiceException>) () -> post(request),
                kq.getCommandType().getActivityTypeSuffix().concat(".executeToJsonResult"));
    }

    @Override
    public Mono<String> executeToJsonResultAsync(KustoQuery kq) {
        return null;
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
        KustoQuery kq = new KustoQuery(command, database, properties);
        return executeToJsonResult(kq);
    }

    private void validateEndpoint() throws DataServiceException, DataClientException {
        try {
            if (!endpointValidated) {
                KustoTrustedEndpoints.validateTrustedEndpoint(clusterUrl,
                        CloudInfo.retrieveCloudInfoForCluster(clusterUrl).getLoginEndpoint());
                endpointValidated = true;
            }
        } catch (KustoClientInvalidConnectionStringException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
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
            boolean leaveOpen) throws DataServiceException, DataClientException {
        boolean isStreamSource = stream != null;

        Map<String, String> headers = new HashMap<>();
        String authorization = getAuthorizationHeaderValue();
        String contentEncoding = null;
        String contentType;
        if (isStreamSource) {
            contentEncoding = "gzip";
        }

        // This was a separate method but was moved into the body of this method because it performs a side effect
        if (properties != null) {
            Iterator<Map.Entry<String, Object>> iterator = properties.getOptions();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> pair = iterator.next();
                headers.put(pair.getKey(), pair.getValue().toString());
            }
        }

        try (InputStream ignored = (isStreamSource && !leaveOpen) ? stream : null) {
            // Validate the endpoint
            validateEndpoint();
            BinaryData data;

            if (isStreamSource) {
                // We use UncloseableStream to prevent HttpClient From closing it
                data = BinaryData.fromStream(new UncloseableStream(stream));
                contentType = "application/octet-stream";
            } else {
                data = BinaryData.fromString(new IngestionSourceStorage(blobUrl).toString());
                contentType = "application/json";
            }

            // Build the tracing object
            HttpTracing tracing = HttpTracing
                    .newBuilder()
                    .withProperties(properties)
                    .withRequestPrefix("KJC.executeStreamingIngest" + (isStreamSource ? "" : "FromBlob"))
                    .withActivitySuffix(CommandType.STREAMING_INGEST.getActivityTypeSuffix())
                    .withClientDetails(clientDetails)
                    .build();

            // Build the HTTP request. Since this is an ingestion and not a command, content headers aren't auto-applied.
            HttpRequest request = HttpRequestBuilder
                    .newPost(clusterEndpoint)
                    .withTracing(tracing)
                    .withHeaders(headers)
                    .withAuthorization(authorization)
                    .withContentType(contentType)
                    .withContentEncoding(contentEncoding)
                    .withBody(data)
                    .build();

            // Get the response, and trace the call.
            String response = MonitoredActivity.invoke(
                    (SupplierOneException<String, DataServiceException>) () -> post(request), "ClientImpl.executeStreamingIngest");

            return new KustoOperationResult(response, "v1");

        } catch (KustoServiceQueryError e) {
            throw new DataClientException(clusterEndpoint, "Error converting json response to KustoOperationResult:" + e.getMessage(), e);
        } catch (IOException e) {
            throw new DataClientException(clusterUrl, e.getMessage(), e);
        }
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
        String clusterEndpoint = String.format(commandType.getEndpoint(), clusterUrl);
        String authorization = getAuthorizationHeaderValue();

        // Validate the endpoint
        validateEndpoint();

        // Build the tracing object
        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(properties)
                .withRequestPrefix("KJC.executeStreaming")
                .withActivitySuffix(commandType.getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        // Build the HTTP request
        HttpRequest request = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .createCommandPayload(database, command, properties)
                .withTracing(tracing)
                .withAuthorization(authorization)
                .build();

        // Get the response and trace the call
        return MonitoredActivity.invoke(
                (SupplierOneException<InputStream, DataServiceException>) () -> postToStreamingOutput(request),
                "ClientImpl.executeStreamingQuery", updateAndGetExecuteTracingAttributes(database, properties));
    }

    private CommandType determineCommandType(String command) {
        if (command.startsWith(ADMIN_COMMANDS_PREFIX)) {
            return CommandType.ADMIN_COMMAND;
        }
        return CommandType.QUERY;
    }

    private String getAuthorizationHeaderValue() throws DataServiceException, DataClientException {
        if (aadAuthenticationHelper != null) {
            return String.format("Bearer %s", aadAuthenticationHelper.acquireAccessToken());
        }
        return null;
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    ClientDetails getClientDetails() {
        return clientDetails;
    }

    // No implementation as the HTTP Client is no longer a closeable
    @Override
    public void close() throws IOException {
    }

}
