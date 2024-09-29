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
import com.microsoft.azure.kusto.data.instrumentation.*;
import com.microsoft.azure.kusto.data.req.KustoRequest;
import com.microsoft.azure.kusto.data.req.KustoRequestContext;
import com.microsoft.azure.kusto.data.res.JsonResult;
import org.apache.commons.lang3.StringUtils;

import org.jetbrains.annotations.NotNull;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.*;

import static reactor.core.publisher.Mono.just;

class ClientImpl extends BaseClient {
    public static final String MGMT_ENDPOINT_VERSION = "v1";
    public static final String QUERY_ENDPOINT_VERSION = "v2";
    public static final String STREAMING_VERSION = "v1";
    private static final String DEFAULT_DATABASE_NAME = "NetDefaultDb";

    private final TokenProviderBase aadAuthenticationHelper;

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
        String clusterURL = UriUtils.createClusterURLFrom(csb.getClusterUrl());
        csb.setClusterUrl(clusterURL);

        clusterUrl = csb.getClusterUrl();
        aadAuthenticationHelper = clusterUrl.toLowerCase().startsWith(CloudInfo.LOCALHOST) ? null : TokenProviderFactory.createTokenProvider(csb, httpClient);
        clientDetails = new ClientDetails(csb.getApplicationNameForTracing(), csb.getUserNameForTracing(), csb.getClientVersionForTracing());
    }

    // @Override
    // public Mono<KustoOperationResult> executeQueryAsync(String database, String command, ClientRequestProperties properties) {
    // KustoRequest kr = new KustoRequest(command, database, properties);
    // return executeQueryAsync(kr);
    // }
    //
    // Mono<KustoOperationResult> executeQueryAsync(@NotNull KustoRequest kr) {
    // if (kr.getCommandType() != CommandType.QUERY) {
    // kr.setCommandType(CommandType.QUERY);
    // }
    // return executeAsync(kr);
    // }
    //
    // @Override
    // public Mono<KustoOperationResult> executeMgmtAsync(String database, String command, ClientRequestProperties properties) {
    // KustoRequest kr = new KustoRequest(command, database, properties);
    // return executeMgmtAsync(kr);
    // }
    //
    // public Mono<KustoOperationResult> executeMgmtAsync(@NotNull KustoRequest kr) {
    // if (kr.getCommandType() != CommandType.ADMIN_COMMAND) {
    // kr.setCommandType(CommandType.ADMIN_COMMAND);
    // }
    // return executeAsync(kr);
    // }
    //
    // private Mono<KustoOperationResult> executeAsync(KustoRequest kr) {
    //
    // Mono<String> resultMono = executeToJsonAsync(kr)
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err));
    // Mono<String> endpointMono = Mono.just(String.format(kr.getCommandType().getEndpoint(), clusterUrl))
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err));
    //
    // return Mono.zip(resultMono, endpointMono)
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err))
    // .map(tuple2 -> new JsonResult(tuple2.getT1(), tuple2.getT2()))
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err))
    // .flatMap(this::processJsonResultAsync)
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err));
    // }
    //
    // public Mono<KustoOperationResult> processJsonResultAsync(JsonResult res) {
    // try {
    // return Mono.just(processJsonResult(res));
    // } catch (Exception e) {
    // return Mono.error(new RuntimeException("Error processing json result", e));
    // }
    // }
    //
    // public Mono<String> executeToJsonAsync(String database, String command, ClientRequestProperties properties) {
    // KustoRequest kr = new KustoRequest(command, database, properties);
    // return executeToJsonAsync(kr)
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err));
    // }
    //
    // Mono<String> executeToJsonAsync(KustoRequest kr) {
    // return just(kr)
    // .flatMap(this::prepareRequestAsync)
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err))
    // .flatMap(this::processRequestAsync)
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err));
    // }

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

    private KustoOperationResult execute(String database, String command, ClientRequestProperties properties, CommandType commandType)
            throws DataServiceException, DataClientException {
        KustoRequest kr = new KustoRequest(command, database, properties, commandType);

        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<KustoOperationResult, DataServiceException, DataClientException>) () -> executeImpl(kr),
                commandType.getActivityTypeSuffix().concat(".execute"),
                updateAndGetExecuteTracingAttributes(database, properties));
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

    private KustoOperationResult executeImpl(KustoRequest kr) throws DataServiceException, DataClientException {
        String response = executeToJsonResult(kr);
        String clusterEndpoint = String.format(kr.getCommandType().getEndpoint(), clusterUrl);
        return processJsonResult(new JsonResult(response, clusterEndpoint));
    }

    private KustoOperationResult processJsonResult(JsonResult res) throws DataServiceException, DataClientException {
        try {
            return new KustoOperationResult(res.getResult(), res.getEndpoint().endsWith("v2/rest/query") ? "v2" : "v1");
        } catch (KustoServiceQueryError e) {
            throw new DataServiceException(res.getEndpoint(),
                    "Error found while parsing json response as KustoOperationResult:" + e.getMessage(), e, e.isPermanent());
        } catch (Exception e) {
            throw new DataClientException(res.getEndpoint(), e.getMessage(), e);
        }
    }

    KustoRequestContext prepareRequest(@NotNull KustoRequest kr) throws DataServiceException, DataClientException {

        // Validate and optimize the query object
        kr.validateAndOptimize();

        String clusterEndpoint = String.format(kr.getCommandType().getEndpoint(), clusterUrl);
        String authorization = getAuthorizationHeaderValue();

        // Validate the endpoint (?)
        validateEndpoint();

        // Build the tracing object
        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(kr.getProperties())
                .withRequestPrefix("KJC.execute")
                .withActivitySuffix(kr.getCommandType().getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        // Build the HTTP request
        HttpRequest request = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .createCommandPayload(kr)
                .withTracing(tracing)
                .withAuthorization(authorization)
                .build();

        // Wrap the Http request and SDK request in a singular object, so we can use BiConsumer later.
        return new KustoRequestContext(kr, request);
    }

    public Mono<KustoRequestContext> prepareRequestAsync(@NotNull KustoRequest kr) {
        try {
            return Mono.just(prepareRequest(kr));
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Failed to prepare KustoRequestContext", e));
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
        KustoRequest kr = new KustoRequest(command, database, properties);
        return executeToJsonResult(kr);
    }

    private String executeToJsonResult(KustoRequest kr) throws DataServiceException, DataClientException {

        KustoRequestContext request = prepareRequest(kr);

        // Get the response and trace the call
        return MonitoredActivity.invoke(
                (SupplierOneException<String, DataServiceException>) () -> post(request.getHttpRequest()),
                request.getSdkRequest().getCommandType().getActivityTypeSuffix().concat(".executeToJsonResult"));
    }

    // public Mono<String> processRequestAsync(KustoRequestContext request) {
    // return MonitoredActivity.invoke(
    // (SupplierNoException<Mono<String>>) () -> postAsync(request.getHttpRequest())
    // .onErrorContinue((err, src) -> LOGGER.error("Error coming from src {}", src, err)),
    // request.getSdkRequest().getCommandType().getActivityTypeSuffix().concat(".executeToJsonResult"));
    // }

    private void validateEndpoint() throws DataServiceException, DataClientException {
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
        KustoRequest kr = new KustoRequest(command, database, properties);
        return executeStreamingQuery(kr);
    }

    public InputStream executeStreamingQuery(@NotNull KustoRequest kr) throws DataServiceException, DataClientException {

        // Validate and optimize the query object
        kr.validateAndOptimize();

        String clusterEndpoint = String.format(kr.getCommandType().getEndpoint(), clusterUrl);
        String authorization = getAuthorizationHeaderValue();

        // Validate the endpoint
        validateEndpoint();

        // Build the tracing object
        HttpTracing tracing = HttpTracing
                .newBuilder()
                .withProperties(kr.getProperties())
                .withRequestPrefix("KJC.executeStreaming")
                .withActivitySuffix(kr.getCommandType().getActivityTypeSuffix())
                .withClientDetails(clientDetails)
                .build();

        // Build the HTTP request
        HttpRequest request = HttpRequestBuilder
                .newPost(clusterEndpoint)
                .createCommandPayload(kr)
                .withTracing(tracing)
                .withAuthorization(authorization)
                .build();

        // Get the response and trace the call
        return MonitoredActivity.invoke(
                (SupplierOneException<InputStream, DataServiceException>) () -> postToStreamingOutput(request),
                "ClientImpl.executeStreamingQuery", updateAndGetExecuteTracingAttributes(kr.getDatabase(), kr.getProperties()));
    }

    private String getAuthorizationHeaderValue() throws DataServiceException, DataClientException {
        if (aadAuthenticationHelper != null) {
            return String.format("Bearer %s", aadAuthenticationHelper.acquireAccessToken().block());
        }
        return null;
    }

    public String getClusterUrl() {
        return clusterUrl;
    }

    ClientDetails getClientDetails() {
        return clientDetails;
    }

}
