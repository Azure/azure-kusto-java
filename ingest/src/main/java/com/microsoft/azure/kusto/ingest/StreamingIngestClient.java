// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ExceptionUtils;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.instrumentation.MonitoredActivity;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.IngestionStatusResult;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

public class StreamingIngestClient extends IngestClientBase implements IngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int STREAM_COMPRESS_BUFFER_SIZE = 16 * 1024;
    private static final String CLASS_NAME = StreamingIngestClient.class.getSimpleName();
    private final StreamingClient streamingClient;
    String connectionDataSource;

    StreamingIngestClient(ConnectionStringBuilder csb, @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new StreamingIngestClient");
        ConnectionStringBuilder csbWithEndpoint = new ConnectionStringBuilder(csb);
        csbWithEndpoint.setClusterUrl(autoCorrectEndpoint ? getQueryEndpoint(csbWithEndpoint.getClusterUrl()) : csbWithEndpoint.getClusterUrl());
        this.streamingClient = ClientFactory.createStreamingClient(csbWithEndpoint, properties);
        this.connectionDataSource = csbWithEndpoint.getClusterUrl();
    }

    StreamingIngestClient(ConnectionStringBuilder csb, @Nullable HttpClient httpClient, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new StreamingIngestClient");
        ConnectionStringBuilder csbWithEndpoint = new ConnectionStringBuilder(csb);
        csbWithEndpoint.setClusterUrl(autoCorrectEndpoint ? getQueryEndpoint(csbWithEndpoint.getClusterUrl()) : csbWithEndpoint.getClusterUrl());
        this.streamingClient = ClientFactory.createStreamingClient(csbWithEndpoint, httpClient);
        this.connectionDataSource = csbWithEndpoint.getClusterUrl();
    }

    StreamingIngestClient(StreamingClient streamingClient) {
        log.info("Creating a new StreamingIngestClient");
        this.streamingClient = streamingClient;
    }

    public static String generateEngineUriSuggestion(URI existingEndpoint) throws URISyntaxException {
        if (!Objects.requireNonNull(existingEndpoint.getHost()).toLowerCase().startsWith(IngestClientBase.INGEST_PREFIX)) {
            throw new IllegalArgumentException("The URL is already formatted as the suggested Engine endpoint, so no suggestion can be made");
        }

        String host = existingEndpoint.getHost().substring(IngestClientBase.INGEST_PREFIX.length());
        URI newUri = new URI(
                existingEndpoint.getScheme(),
                host,
                existingEndpoint.getPath(),
                existingEndpoint.getQuery(),
                existingEndpoint.getFragment());

        return newUri.toString();
    }

    @Override
    protected Mono<IngestionResult> ingestFromFileAsyncImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, false);
            return ingestFromStreamAsync(streamSourceInfo, ingestionProperties);
        } catch (IOException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    @Override
    protected Mono<IngestionResult> ingestFromBlobAsyncImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        blobSourceInfo.validate();
        ingestionProperties.validate();

        BlobAsyncClient blobAsyncClient;
        try {
            return ingestFromBlobAsync(blobSourceInfo, ingestionProperties, null)
                    .onErrorMap(BlobStorageException.class, e -> {
                        String msg = "Unexpected Storage error when ingesting a blob.";
                        log.error(msg, e);
                        return new IngestionClientException(msg, e);
                    });
        } catch (IllegalArgumentException e) {
            String msg = "Unexpected error when ingesting a blob - Invalid blob path.";
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        }
    }

    @Override
    protected Mono<IngestionResult> ingestFromResultSetAsyncImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        resultSetSourceInfo.validate();
        ingestionProperties.validateResultSetProperties();

        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.resultSetToStream(resultSetSourceInfo);
            return ingestFromStreamAsync(streamSourceInfo, ingestionProperties);
        } catch (IOException e) {
            String msg = "Failed to read from ResultSet.";
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        }
    }

    @Override
    protected Mono<IngestionResult> ingestFromStreamAsyncImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        return ingestFromStreamImplAsync(streamSourceInfo, ingestionProperties, null);
    }

    @Override
    protected String getClientType() {
        return CLASS_NAME;
    }

    Mono<IngestionResult> ingestFromStreamAsync(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties, @Nullable String clientRequestId) {
        // trace ingestFromStreamAsync
        return MonitoredActivity.wrap(
                ingestFromStreamImplAsync(streamSourceInfo,
                        ingestionProperties, clientRequestId),
                getClientType().concat(".ingestFromStream"),
                getIngestionTraceAttributes(streamSourceInfo, ingestionProperties));
    }

    private Mono<IngestionResult> ingestFromStreamImplAsync(StreamSourceInfo streamSourceInfo,
            IngestionProperties ingestionProperties,
            @Nullable String clientRequestId) {

        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        ingestionProperties.validate();
        streamSourceInfo.validate();

        IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();

        return (IngestClientBase.shouldCompress(streamSourceInfo.getCompressionType(), dataFormat)
                ? IngestionUtils.compressStream(streamSourceInfo.getStream(), streamSourceInfo.isLeaveOpen())
                : Mono.just(streamSourceInfo.getStream()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .onErrorMap(IOException.class, e -> {
                            String msg = ExceptionUtils.getMessageEx(e);
                            log.error(msg, e);
                            return new IngestionClientException(msg, e);
                        })
                        .flatMap(stream -> {
                            ClientRequestProperties clientRequestProperties = null;
                            if (StringUtils.isNotBlank(clientRequestId)) {
                                clientRequestProperties = new ClientRequestProperties();
                                clientRequestProperties.setClientRequestId(clientRequestId);
                            }

                            log.debug("Executing streaming ingest");
                            return this.streamingClient.executeStreamingIngestAsync(
                                    ingestionProperties.getDatabaseName(),
                                    ingestionProperties.getTableName(),
                                    stream,
                                    clientRequestProperties,
                                    dataFormat.getKustoValue(),
                                    ingestionProperties.getIngestionMapping().getIngestionMappingReference(),
                                    !(streamSourceInfo.getCompressionType() == null || !streamSourceInfo.isLeaveOpen()))
                                    .doOnSuccess(ignored -> log.debug("Stream was ingested successfully."));
                        })
                        .onErrorMap(DataClientException.class, e -> {
                            String msg = ExceptionUtils.getMessageEx(e);
                            log.error(msg, e);
                            return new IngestionClientException(msg, e);
                        })
                        .onErrorMap(DataServiceException.class, e -> {
                            log.error(e.getMessage(), e);
                            return new IngestionServiceException(e.getMessage(), e);
                        })
                        .map(ignore -> {
                            log.debug("Stream was ingested successfully.");
                            IngestionStatus ingestionStatus = new IngestionStatus();
                            ingestionStatus.status = OperationStatus.Succeeded;
                            ingestionStatus.table = ingestionProperties.getTableName();
                            ingestionStatus.database = ingestionProperties.getDatabaseName();
                            return new IngestionStatusResult(ingestionStatus);
                        });
    }

    Mono<IngestionResult> ingestFromBlobAsync(BlobSourceInfo blobSourceInfo,
            IngestionProperties ingestionProperties,
            @Nullable String clientRequestId) {
        // trace ingestFromBlobAsync
        return MonitoredActivity.wrap(
                ingestFromBlobImplAsync(blobSourceInfo,
                        ingestionProperties, clientRequestId),
                getClientType().concat(".ingestFromBlob"),
                getIngestionTraceAttributes(blobSourceInfo, ingestionProperties));
    }

    private Mono<IngestionResult> ingestFromBlobImplAsync(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties,
            @Nullable String clientRequestId) {

        String blobPath = blobSourceInfo.getBlobPath();
        ClientRequestProperties clientRequestProperties = null;
        if (StringUtils.isNotBlank(clientRequestId)) {
            clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setClientRequestId(clientRequestId);
        }

        IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();
        return this.streamingClient.executeStreamingIngestFromBlobAsync(ingestionProperties.getDatabaseName(),
                ingestionProperties.getTableName(),
                blobPath,
                clientRequestProperties,
                dataFormat.getKustoValue(),
                ingestionProperties.getIngestionMapping().getIngestionMappingReference())
            .onErrorMap(DataClientException.class, e -> {
                    log.error(e.getMessage(), e);
                    return new IngestionClientException(e.getMessage(), e);
                })
                .onErrorMap(DataServiceException.class, e -> {
                    log.error(e.getMessage(), e);
                    return new IngestionServiceException(e.getMessage(), e);
                })
                .doOnSuccess(ignored1 -> log.debug("Blob was ingested successfully."))
                .map(ignore -> {
                    IngestionStatus ingestionStatus = new IngestionStatus();
                    ingestionStatus.status = OperationStatus.Succeeded;
                    ingestionStatus.table = ingestionProperties.getTableName();
                    ingestionStatus.database = ingestionProperties.getDatabaseName();
                    return new IngestionStatusResult(ingestionStatus);
                });
    }

    protected void setConnectionDataSource(String connectionDataSource) {
        this.connectionDataSource = connectionDataSource;
    }

    @Override
    public void close() {
    }
}
