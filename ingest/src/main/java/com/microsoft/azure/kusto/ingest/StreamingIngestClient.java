// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ClientRequestProperties;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.ExceptionsUtils;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.data.http.HttpStatus;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

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
        return Mono.fromCallable(() -> {
                    Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
                    Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
                    fileSourceInfo.validate();
                    ingestionProperties.validate();
                    return IngestionUtils.fileToStream(fileSourceInfo, false, ingestionProperties.getDataFormat());
                })
                .onErrorMap(FileNotFoundException.class, e -> {
                    log.error("File not found when ingesting a file.", e);
                    return new IngestionClientException("IO exception - check file path.", e);
                })
                .flatMap(streamSourceInfo -> ingestFromStreamAsync(streamSourceInfo, ingestionProperties));
    }

    @Override
    protected Mono<IngestionResult> ingestFromBlobAsyncImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) {
        return Mono.fromCallable(() -> {
                    Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
                    Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
                    blobSourceInfo.validate();
                    ingestionProperties.validate();
                    return true;
                })
                .flatMap(valid -> {
                    BlobClient blobClient;
                    try {
                        blobClient = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath()).buildClient();
                    } catch (IllegalArgumentException e) {

                        // Handle IllegalArgumentException from BlobClient here to avoid overriding the exception
                        // thrown by the argument validations.
                        String msg = "Unexpected error when ingesting a blob - Invalid blob path.";
                        log.error(msg, e);
                        return Mono.error(new IngestionClientException(msg, e));
                    }

                    return ingestFromBlobAsync(blobSourceInfo, ingestionProperties, blobClient, null);
                })
                .onErrorMap(BlobStorageException.class, e -> {
                    String msg = "Unexpected Storage error when ingesting a blob.";
                    log.error(msg, e);
                    return new IngestionClientException(msg, e);
                });
    }

    @Override
    protected Mono<IngestionResult> ingestFromResultSetAsyncImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        return Mono.fromCallable(() -> {
                    Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
                    Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
                    resultSetSourceInfo.validate();
                    ingestionProperties.validateResultSetProperties();
                    return IngestionUtils.resultSetToStream(resultSetSourceInfo);
                })
                .flatMap(streamSourceInfo -> ingestFromStreamAsync(streamSourceInfo, ingestionProperties))
                .onErrorMap(IOException.class, e -> {
                    String msg = "Failed to read from ResultSet.";
                    log.error(msg, e);
                    return new IngestionClientException(msg, e);
                });
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
        return Mono.fromCallable(() -> {
                    Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
                    Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
                    ingestionProperties.validate();
                    streamSourceInfo.validate();

                    return ingestionProperties.getDataFormat();
                })
                .onErrorMap(IOException.class, e -> {
                    String msg = ExceptionsUtils.getMessageEx(e);
                    log.error(msg, e);
                    return new IngestionClientException(msg, e);
                })
                .flatMap(dataFormat -> Mono.fromCallable(() -> {
                                    if (IngestClientBase.shouldCompress(streamSourceInfo.getCompressionType(), dataFormat)) {
                                        return compressStream(streamSourceInfo.getStream(), streamSourceInfo.isLeaveOpen());
                                    } else {
                                        return streamSourceInfo.getStream();
                                    }
                                })
                                .subscribeOn(Schedulers.boundedElastic())
                                .map(stream -> new Object[]{stream, dataFormat}) // Pass both stream and dataFormat downstream
                )
                .flatMap(tuple -> {
                    InputStream stream = (InputStream) tuple[0];
                    IngestionProperties.DataFormat dataFormat = (IngestionProperties.DataFormat) tuple[1];

                    ClientRequestProperties clientRequestProperties = null;
                    if (StringUtils.isNotBlank(clientRequestId)) {
                        clientRequestProperties = new ClientRequestProperties();
                        clientRequestProperties.setClientRequestId(clientRequestId);
                    }

                    ClientRequestProperties finalClientRequestProperties = clientRequestProperties;
                    return Mono.fromCallable(() -> {
                                log.debug("Executing streaming ingest");
                                return this.streamingClient.executeStreamingIngestAsync(
                                        ingestionProperties.getDatabaseName(),
                                        ingestionProperties.getTableName(),
                                        stream,
                                        finalClientRequestProperties,
                                        dataFormat.getKustoValue(),
                                        ingestionProperties.getIngestionMapping().getIngestionMappingReference(),
                                        !(streamSourceInfo.getCompressionType() == null || !streamSourceInfo.isLeaveOpen())
                                );
                            })
                            .flatMap(Function.identity())
                            .doOnSuccess(ignored -> log.debug("Stream was ingested successfully."))
                            .then(Mono.fromCallable(() -> {
                                log.debug("Stream was ingested successfully.");
                                IngestionStatus ingestionStatus = new IngestionStatus();
                                ingestionStatus.status = OperationStatus.Succeeded;
                                ingestionStatus.table = ingestionProperties.getTableName();
                                ingestionStatus.database = ingestionProperties.getDatabaseName();
                                return (IngestionResult) new IngestionStatusResult(ingestionStatus);
                            }))
                            .onErrorMap(DataClientException.class, e -> {
                                String msg = ExceptionsUtils.getMessageEx(e);
                                log.error(msg, e);
                                return new IngestionClientException(msg, e);
                            })
                            .onErrorMap(DataServiceException.class, e -> {
                                log.error(e.getMessage(), e);
                                return new IngestionServiceException(e.getMessage(), e);
                            });
                });
    }

    private InputStream compressStream(InputStream uncompressedStream, boolean leaveOpen) throws IngestionClientException, IOException {
        log.debug("Compressing the stream.");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream);
        byte[] b = new byte[STREAM_COMPRESS_BUFFER_SIZE];
        int read = uncompressedStream.read(b);
        if (read == -1) {
            String message = "Empty stream.";
            log.error(message);
            throw new IngestionClientException(message);
        }
        do {
            gzipOutputStream.write(b, 0, read);
        } while ((read = uncompressedStream.read(b)) != -1);
        gzipOutputStream.flush();
        gzipOutputStream.close();
        InputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        byteArrayOutputStream.close();
        if (!leaveOpen) {
            uncompressedStream.close();
        }
        return inputStream;
    }

    Mono<IngestionResult> ingestFromBlobAsync(BlobSourceInfo blobSourceInfo,
                                              IngestionProperties ingestionProperties,
                                              BlobClient cloudBlockBlob,
                                              @Nullable String clientRequestId) {
        // trace ingestFromBlobAsync
        return MonitoredActivity.wrap(
                ingestFromBlobImplAsync(blobSourceInfo,
                        ingestionProperties, cloudBlockBlob, clientRequestId),
                getClientType().concat(".ingestFromBlob"),
                getIngestionTraceAttributes(blobSourceInfo, ingestionProperties));
    }

    private Mono<IngestionResult> ingestFromBlobImplAsync(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties, BlobClient cloudBlockBlob,
                                                          @Nullable String clientRequestId) {

        return Mono.fromCallable(() -> {
                    if (blobSourceInfo.getRawSizeInBytes() == 0 && cloudBlockBlob.getProperties().getBlobSize() == 0) {
                        String message = "Empty blob.";
                        log.error(message);
                        throw new IngestionClientException(message);
                    }
                    return true;
                }).subscribeOn(Schedulers.boundedElastic())//TODO: same
                .onErrorMap(BlobStorageException.class, e -> new IngestionClientException(String.format("Exception trying to read blob metadata,%s",
                        e.getStatusCode() == HttpStatus.FORBIDDEN ? "this might mean the blob doesn't exist" : ""), e))
                .flatMap(ignored -> Mono.fromCallable(() -> {
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
                                    ingestionProperties.getIngestionMapping().getIngestionMappingReference());
                        })
                        .onErrorMap(DataClientException.class, e -> {
                            log.error(e.getMessage(), e);
                            return new IngestionClientException(e.getMessage(), e);
                        })
                        .onErrorMap(DataServiceException.class, e -> {
                            log.error(e.getMessage(), e);
                            return new IngestionServiceException(e.getMessage(), e);
                        })
                        .doOnSuccess(ignored1 -> log.debug("Blob was ingested successfully."))
                        .then(Mono.fromCallable(() -> {
                            IngestionStatus ingestionStatus = new IngestionStatus();
                            ingestionStatus.status = OperationStatus.Succeeded;
                            ingestionStatus.table = ingestionProperties.getTableName();
                            ingestionStatus.database = ingestionProperties.getDatabaseName();
                            return new IngestionStatusResult(ingestionStatus);
                        })));
    }

    protected void setConnectionDataSource(String connectionDataSource) {
        this.connectionDataSource = connectionDataSource;
    }

    @Override
    public void close() {
    }
}
