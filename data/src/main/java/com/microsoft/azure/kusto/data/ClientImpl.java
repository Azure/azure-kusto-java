// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import com.azure.core.http.HttpClient;
import com.azure.core.util.BinaryData;
import com.microsoft.azure.kusto.data.auth.CloudInfo;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.auth.TokenProviderBase;
import com.microsoft.azure.kusto.data.auth.TokenProviderFactory;
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.data.exceptions.KustoServiceQueryError;
import com.microsoft.azure.kusto.data.exceptions.ParseException;
import com.microsoft.azure.kusto.data.http.HttpClientFactory;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.http.HttpRequestBuilder;
import com.microsoft.azure.kusto.data.http.HttpTracing;
import com.microsoft.azure.kusto.data.http.UncloseableStream;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.data.instrumentation.TraceableAttributes;
import com.microsoft.azure.kusto.data.req.KustoRequest;
import com.microsoft.azure.kusto.data.req.KustoRequestContext;
import com.microsoft.azure.kusto.data.res.JsonResult;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

class ClientImpl extends BaseClient {
    public static final String MGMT_ENDPOINT_VERSION = "v1";
    public static final String QUERY_ENDPOINT_VERSION = "v2";
    public static final String STREAMING_VERSION = "v1";
    private static final Long CLIENT_GRACE_PERIOD_IN_MILLISECS = TimeUnit.SECONDS.toMillis(30);
    private static final Long COMMAND_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);
    private static final Long QUERY_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(4);
    private static final Long STREAMING_INGEST_TIMEOUT_IN_MILLISECS = TimeUnit.MINUTES.toMillis(10);

    private final TokenProviderBase aadAuthenticationHelper;

    private final String clusterUrl;
    private final String defaultDatabaseName;
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
        String clusterURL = UriUtils.createClusterURLFrom(csb.getClusterUrl());
        csb.setClusterUrl(clusterURL);
        clusterUrl = csb.getClusterUrl();
        aadAuthenticationHelper = clusterUrl.toLowerCase().startsWith(CloudInfo.LOCALHOST) ? null : TokenProviderFactory.createTokenProvider(csb, httpClient);
        clientDetails = new ClientDetails(csb.getApplicationNameForTracing(), csb.getUserNameForTracing(), csb.getClientVersionForTracing());
        defaultDatabaseName = csb.getInitialCatalog();
    }

    @Override
    public KustoOperationResult executeQuery(String query) {
        return executeQuery(defaultDatabaseName, query);
    }

    @Override
    public KustoOperationResult executeQuery(String database, String query) {
        return executeQuery(database, query, null);
    }

    @Override
    public KustoOperationResult executeQuery(String database, String query, ClientRequestProperties properties) {
        return executeQueryAsync(database, query, properties).block();
    }

    @Override
    public Mono<KustoOperationResult> executeQueryAsync(String query) {
        return executeQueryAsync(defaultDatabaseName, query);
    }

    @Override
    public Mono<KustoOperationResult> executeQueryAsync(String database, String query) {
        return executeQueryAsync(database, query, null);
    }

    @Override
    public Mono<KustoOperationResult> executeQueryAsync(String database, String query, ClientRequestProperties properties) {
        return executeAsync(database, query, properties, CommandType.QUERY);
    }

    @Override
    public KustoOperationResult executeMgmt(String command) {
        return executeMgmt(defaultDatabaseName, command);
    }

    @Override
    public KustoOperationResult executeMgmt(String database, String command) {
        return executeMgmt(database, command, null);
    }

    @Override
    public KustoOperationResult executeMgmt(String database, String command, ClientRequestProperties properties) {
        return executeAsync(database, command, properties, CommandType.ADMIN_COMMAND).block();
    }

    @Override
    public Mono<KustoOperationResult> executeMgmtAsync(String command) {
        return executeMgmtAsync(defaultDatabaseName, command);
    }

    @Override
    public Mono<KustoOperationResult> executeMgmtAsync(String database, String command) {
        return executeMgmtAsync(defaultDatabaseName, command, null);
    }

    @Override
    public Mono<KustoOperationResult> executeMgmtAsync(String database, String command, ClientRequestProperties properties) {
        return executeAsync(database, command, properties, CommandType.ADMIN_COMMAND);
    }

    @Override
    public String executeToJsonResult(String database, String command, ClientRequestProperties properties) {
        return executeToJsonResultAsync(database, command, properties).block();
    }

    @Override
    public Mono<String> executeToJsonResultAsync(String database, String command, ClientRequestProperties properties) {
        return Mono.defer(() -> {
            KustoRequest kr = new KustoRequest(command, database == null ? defaultDatabaseName : database, properties);
            return executeWithTimeout(kr, ".executeToJsonResultAsync");
        });
    }

    private Mono<KustoOperationResult> executeAsync(String database, String command, ClientRequestProperties properties, CommandType commandType) {
        return Mono.defer(() -> {
            KustoRequest kr = new KustoRequest(command, database, properties, commandType);
            return MonitoredActivity.wrap(
                    executeImplAsync(kr),
                    commandType.getActivityTypeSuffix().concat(".executeAsync"),
                    updateAndGetExecuteTracingAttributes(database, properties));
        });
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

    private Mono<KustoOperationResult> executeImplAsync(KustoRequest kr) {
        String clusterEndpoint = String.format(kr.getCommandType().getEndpoint(), clusterUrl);
        return executeWithTimeout(kr, ".executeImplAsync")
                .publishOn(Schedulers.boundedElastic())
                .map(response -> {
                    JsonResult jsonResult = new JsonResult(response, clusterEndpoint);
                    return new KustoOperationResult(jsonResult.getResult(), jsonResult.getEndpoint().endsWith("v2/rest/query") ? "v2" : "v1");
                })
                .onErrorMap(KustoServiceQueryError.class, e -> new DataServiceException(clusterEndpoint, e.getMessage(), e, e.isPermanent()))
                .onErrorMap(Exception.class, e -> {
                    if (e instanceof DataServiceException) {
                        return e;
                    }

                    return new DataClientException(clusterEndpoint, ExceptionUtils.getMessageEx(e), e);
                });
    }

    private Mono<String> executeWithTimeout(KustoRequest request, String nameOfSpan) {
        return prepareRequestAsync(request)
                .zipWhen(requestContext -> {
                    ClientRequestProperties properties = request.getProperties() == null ? new ClientRequestProperties() : request.getProperties();
                    long timeoutMs = determineTimeout(properties, request.getCommandType(), clusterUrl);
                    return MonitoredActivity.wrap(
                            postAsync(requestContext.getHttpRequest(), timeoutMs),
                            requestContext.getSdkRequest().getCommandType().getActivityTypeSuffix().concat(nameOfSpan));
                })
                .map(Tuple2::getT2);
    }

    Mono<KustoRequestContext> prepareRequestAsync(@NotNull KustoRequest kr) {
        kr.validateAndOptimize();

        String clusterEndpoint = String.format(kr.getCommandType().getEndpoint(), clusterUrl);
        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(kr.getProperties())
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix(kr.getCommandType().getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        HttpRequestBuilder requestBuilder = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .createCommandPayload(kr)
                .withTracing(tracing);

        return validateEndpointAsync()
                .then(getAuthorizationHeaderValueAsync()
                        .doOnNext(requestBuilder::withAuthorization)
                        .thenReturn(new KustoRequestContext(kr, requestBuilder.build())));
    }

    private Mono<Void> validateEndpointAsync() {
        if (endpointValidated) {
            return Mono.empty();
        }

        return CloudInfo.retrieveCloudInfoForClusterAsync(clusterUrl, this.httpClient)
                .map(CloudInfo::getLoginEndpoint)
                .doOnNext(loginEndpoint -> KustoTrustedEndpoints.validateTrustedEndpoint(clusterUrl, loginEndpoint))
                .doOnSuccess(ignored -> endpointValidated = true)
                .then();
    }

    @Override
    public KustoOperationResult executeStreamingIngest(String database, String table, InputStream stream, ClientRequestProperties properties,
            String streamFormat, String mappingName, boolean leaveOpen) {
        return executeStreamingIngestAsync(database, table, stream, properties, streamFormat, mappingName, leaveOpen).block();
    }

    @Override
    public Mono<KustoOperationResult> executeStreamingIngestAsync(String database, String table, InputStream stream, ClientRequestProperties properties,
            String streamFormat, String mappingName, boolean leaveOpen) {
        Ensure.argIsNotNull(stream, "stream");

        return Mono.defer(() -> {
            String clusterEndpoint = buildClusterEndpoint(database, table, streamFormat, mappingName);
            return executeStreamingIngestImplAsync(clusterEndpoint, stream, null, properties, leaveOpen);
        });
    }

    @Override
    public KustoOperationResult executeStreamingIngestFromBlob(String database, String table, String blobUrl, ClientRequestProperties properties,
            String dataFormat, String mappingName) {
        return executeStreamingIngestFromBlobAsync(database, table, blobUrl, properties, dataFormat, mappingName).block();
    }

    @Override
    public Mono<KustoOperationResult> executeStreamingIngestFromBlobAsync(String database, String table, String blobUrl, ClientRequestProperties properties,
            String dataFormat, String mappingName) {
        Ensure.argIsNotNull(blobUrl, "blobUrl");

        return Mono.defer(() -> {
            String clusterEndpoint = buildClusterEndpoint(database, table, dataFormat, mappingName)
                    .concat("&sourceKind=uri");
            return executeStreamingIngestImplAsync(clusterEndpoint, null, blobUrl, properties, false);
        });
    }

    private Mono<KustoOperationResult> executeStreamingIngestImplAsync(String clusterEndpoint, InputStream stream, String blobUrl,
            ClientRequestProperties properties, boolean leaveOpen) {
        return validateEndpointAsync().then(executeStreamingIngest(clusterEndpoint, stream, blobUrl, properties, leaveOpen));
    }

    private Mono<KustoOperationResult> executeStreamingIngest(String clusterEndpoint, InputStream stream, String blobUrl,
            ClientRequestProperties properties, boolean leaveOpen) {
        boolean isStreamSource = stream != null;
        Map<String, String> headers = new HashMap<>();
        String contentEncoding = isStreamSource ? "gzip" : null;
        String contentType = isStreamSource ? "application/octet-stream" : "application/json";

        properties = properties == null ? new ClientRequestProperties() : properties;

        long timeoutMs = determineTimeout(properties, CommandType.STREAMING_INGEST, clusterUrl);

        // This was a separate method but was moved into the body of this method because it performs a side effect
        Iterator<Map.Entry<String, Object>> iterator = properties.getOptions();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> pair = iterator.next();
            headers.put(pair.getKey(), pair.getValue().toString());
        }

        BinaryData data;
        if (isStreamSource) {
            // We use UncloseableStream to prevent HttpClient from closing the stream
            data = BinaryData.fromStream(new UncloseableStream(stream));
        } else {
            data = BinaryData.fromString(new IngestionSourceStorage(blobUrl).toString());
        }

        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(properties)
                .withRequestPrefix("KJC.executeStreamingIngest" + (isStreamSource ? "" : "FromBlob"))
                .withActivitySuffix(CommandType.STREAMING_INGEST.getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        HttpRequestBuilder httpRequestBuilder = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .withTracing(tracing)
                .withHeaders(headers)
                .withContentType(contentType)
                .withContentEncoding(contentEncoding)
                .withBody(data);

        return getAuthorizationHeaderValueAsync()
                .doOnNext(httpRequestBuilder::withAuthorization)
                .then(MonitoredActivity.wrap(postAsync(httpRequestBuilder.build(), timeoutMs), "ClientImpl.executeStreamingIngest")
                        .publishOn(Schedulers.boundedElastic())
                        .map(response -> new KustoOperationResult(response, "v1"))
                        .onErrorMap(KustoServiceQueryError.class, e -> new DataClientException(clusterEndpoint, e.getMessage(), e))
                        .doFinally(signalType -> {
                            if (isStreamSource && !leaveOpen) {
                                try {
                                    stream.close();
                                } catch (IOException e) {
                                    LOGGER.debug("executeStreamingIngest: Error while closing the stream.", e);
                                }
                            }
                        }));
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
    public InputStream executeStreamingQuery(String command) {
        return executeStreamingQuery(defaultDatabaseName, command);
    }

    @Override
    public InputStream executeStreamingQuery(String database, String command) {
        return executeStreamingQuery(database, command, null);
    }

    @Override
    public InputStream executeStreamingQuery(String database, String command, ClientRequestProperties properties) {
        return executeStreamingQueryAsync(database, command, properties).block();
    }

    @Override
    public Mono<InputStream> executeStreamingQueryAsync(String command) {
        return executeStreamingQueryAsync(defaultDatabaseName, command);
    }

    @Override
    public Mono<InputStream> executeStreamingQueryAsync(String database, String command) {
        return executeStreamingQueryAsync(database, command, null);
    }

    @Override
    public Mono<InputStream> executeStreamingQueryAsync(String database, String command, ClientRequestProperties properties) {
        KustoRequest kr = new KustoRequest(command, database, properties);
        return executeStreamingQueryAsync(kr);
    }

    private Mono<InputStream> executeStreamingQueryAsync(@NotNull KustoRequest kr) {
        kr.validateAndOptimize();
        String clusterEndpoint = String.format(kr.getCommandType().getEndpoint(), clusterUrl);
        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(kr.getProperties())
                .withRequestPrefix("KJC.executeStreaming")
                .withActivitySuffix(kr.getCommandType().getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        return validateEndpointAsync().then(executeStreamingQuery(clusterEndpoint, kr, tracing));
    }

    private Mono<InputStream> executeStreamingQuery(String clusterEndpoint, KustoRequest kr,
            HttpTracing tracing) {
        HttpRequestBuilder requestBuilder = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .createCommandPayload(kr)
                .withTracing(tracing);
        ClientRequestProperties properties = kr.getProperties() == null ? new ClientRequestProperties() : kr.getProperties();
        long timeoutMs = determineTimeout(properties, kr.getCommandType(), clusterUrl);

        return getAuthorizationHeaderValueAsync()
                .doOnNext(requestBuilder::withAuthorization)
                .then(MonitoredActivity.wrap(
                        postToStreamingOutputAsync(requestBuilder.build(), timeoutMs, 0,
                                kr.getRedirectCount()),
                        "ClientImpl.executeStreamingQuery", updateAndGetExecuteTracingAttributes(kr.getDatabase(), properties)));
    }

    private long determineTimeout(ClientRequestProperties properties, CommandType commandType, String clusterUrl) {
        Object skipBoolean = properties.getOption(ClientRequestProperties.OPTION_NO_REQUEST_TIMEOUT);
        if (skipBoolean instanceof Boolean && (Boolean) skipBoolean) {
            return Long.MAX_VALUE;
        }

        Long timeoutMs;
        try {
            timeoutMs = properties.getTimeoutInMilliSec();
        } catch (ParseException e) {
            throw new DataClientException(clusterUrl, "Failed to parse timeout from ClientRequestProperties");
        }

        if (timeoutMs == null) {
            switch (commandType) {
                case ADMIN_COMMAND:
                    timeoutMs = COMMAND_TIMEOUT_IN_MILLISECS;
                    break;
                case STREAMING_INGEST:
                    timeoutMs = STREAMING_INGEST_TIMEOUT_IN_MILLISECS;
                    break;
                default:
                    timeoutMs = QUERY_TIMEOUT_IN_MILLISECS;
            }
        }
        // If we set the timeout ourself, we need to update the server header
        properties.setTimeoutInMilliSec(timeoutMs);

        return timeoutMs + CLIENT_GRACE_PERIOD_IN_MILLISECS;
    }

    private Mono<String> getAuthorizationHeaderValueAsync() {
        if (aadAuthenticationHelper != null) {
            return aadAuthenticationHelper.acquireAccessToken()
                    .map(token -> String.format("Bearer %s", token));
        }

        return Mono.empty();
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    ClientDetails getClientDetails() {
        return clientDetails;
    }
}
