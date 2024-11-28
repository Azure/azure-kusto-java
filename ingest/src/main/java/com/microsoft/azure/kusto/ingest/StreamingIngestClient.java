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
import com.microsoft.azure.kusto.data.instrumentation.SupplierTwoExceptions;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
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
    protected IngestionResult ingestFromFileImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, false, ingestionProperties.getDataFormat());
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    @Override
    protected Mono<IngestionResult> ingestFromFileAsyncImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        try {
            fileSourceInfo.validate();
            ingestionProperties.validate();
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, false, ingestionProperties.getDataFormat());
            return ingestFromStreamAsync(streamSourceInfo, ingestionProperties);
        } catch (IngestionClientException e) {
            log.error("Error while validating the ingestion mapping properties.", e);
            return Mono.error(new IngestionClientException("IO exception - check file path.", e));
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            return Mono.error(new IngestionClientException("IO exception - check file path.", e));
        }
    }

    @Override
    protected IngestionResult ingestFromBlobImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        try {
            BlobClient blobClient = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath()).buildClient();
            return ingestFromBlob(blobSourceInfo, ingestionProperties, blobClient, null);
        } catch (IllegalArgumentException e) {
            String msg = "Unexpected error when ingesting a blob - Invalid blob path.";
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        } catch (BlobStorageException e) {
            String msg = "Unexpected Storage error when ingesting a blob.";
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        }
    }

    @Override
    protected Mono<IngestionResult> ingestFromBlobAsyncImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        try {
            blobSourceInfo.validate();
            ingestionProperties.validate();

            BlobClient blobClient = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath()).buildClient();
            return ingestFromBlobAsync(blobSourceInfo, ingestionProperties, blobClient, null);
        } catch (IllegalArgumentException e) {
            String msg = "Unexpected error when ingesting a blob - Invalid blob path.";
            log.error(msg, e);
            return Mono.error(new IngestionClientException(msg, e));
        } catch (BlobStorageException e) {
            String msg = "Unexpected Storage error when ingesting a blob.";
            log.error(msg, e);
            return Mono.error(new IngestionClientException(msg, e));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    protected IngestionResult ingestFromResultSetImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validateResultSetProperties();

        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.resultSetToStream(resultSetSourceInfo);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (IOException ex) {
            String msg = "Failed to read from ResultSet.";
            log.error(msg, ex);
            throw new IngestionClientException(msg, ex);
        }
    }

    @Override
    protected Mono<IngestionResult> ingestFromResultSetAsyncImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        try {
            resultSetSourceInfo.validate();
            ingestionProperties.validateResultSetProperties();
            StreamSourceInfo streamSourceInfo = IngestionUtils.resultSetToStream(resultSetSourceInfo);
            return ingestFromStreamAsync(streamSourceInfo, ingestionProperties);
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    @Override
    protected IngestionResult ingestFromStreamImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        return ingestFromStreamImpl(streamSourceInfo, ingestionProperties, null);
    }

    @Override
    protected Mono<IngestionResult> ingestFromStreamAsyncImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        return ingestFromStreamImplAsync(streamSourceInfo, ingestionProperties, null);
    }

    @Override
    protected String getClientType() {
        return CLASS_NAME;
    }

    IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties, @Nullable String clientRequestId)
            throws IngestionClientException, IngestionServiceException {
        // trace ingestFromStream
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<IngestionResult, IngestionClientException, IngestionServiceException>) () -> ingestFromStreamImpl(streamSourceInfo,
                        ingestionProperties, clientRequestId),
                getClientType().concat(".ingestFromStream"),
                getIngestionTraceAttributes(streamSourceInfo, ingestionProperties));
    }

    Mono<IngestionResult> ingestFromStreamAsync(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties, @Nullable String clientRequestId) {
        // trace ingestFromStreamAsync
        return MonitoredActivity.wrap(
                ingestFromStreamImplAsync(streamSourceInfo,
                        ingestionProperties, clientRequestId),
                getClientType().concat(".ingestFromStream"),
                getIngestionTraceAttributes(streamSourceInfo, ingestionProperties));
    }

    private IngestionResult ingestFromStreamImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties, @Nullable String clientRequestId)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();

        streamSourceInfo.validate();
        ingestionProperties.validate();

        ClientRequestProperties clientRequestProperties = null;
        if (StringUtils.isNotBlank(clientRequestId)) {
            clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setClientRequestId(clientRequestId);
        }

        try {
            InputStream stream = IngestClientBase.shouldCompress(streamSourceInfo.getCompressionType(), dataFormat)
                    ? compressStream(streamSourceInfo.getStream(), streamSourceInfo.isLeaveOpen())
                    : streamSourceInfo.getStream();
            log.debug("Executing streaming ingest");
            this.streamingClient.executeStreamingIngest(ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    stream,
                    clientRequestProperties,
                    dataFormat.getKustoValue(),
                    ingestionProperties.getIngestionMapping().getIngestionMappingReference(),
                    !(streamSourceInfo.getCompressionType() == null || !streamSourceInfo.isLeaveOpen()));
        } catch (DataClientException | IOException e) {
            String msg = ExceptionsUtils.getMessageEx(e);
            log.error(msg, e);
            throw new IngestionClientException(msg, e);
        } catch (DataServiceException e) {
            log.error(e.getMessage(), e);
            throw new IngestionServiceException(e.getMessage(), e);
        }

        log.debug("Stream was ingested successfully.");
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.status = OperationStatus.Succeeded;
        ingestionStatus.table = ingestionProperties.getTableName();
        ingestionStatus.database = ingestionProperties.getDatabaseName();
        return new IngestionStatusResult(ingestionStatus);
    }

    private Mono<IngestionResult> ingestFromStreamImplAsync(StreamSourceInfo streamSourceInfo,
                                                            IngestionProperties ingestionProperties,
                                                            @Nullable String clientRequestId) {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();

        ClientRequestProperties clientRequestProperties = null;
        if (StringUtils.isNotBlank(clientRequestId)) {
            clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setClientRequestId(clientRequestId);
        }

        try {
            streamSourceInfo.validate();
            ingestionProperties.validate();

            Mono<InputStream> streamMono = Mono.just(streamSourceInfo)
                    .flatMap(source -> {
                        if (IngestClientBase.shouldCompress(source.getCompressionType(), dataFormat)) {
                            return Mono.fromCallable(() -> compressStream(source.getStream(), source.isLeaveOpen()));
                        } else {
                            return Mono.just(source.getStream());
                        }
                    });

            ClientRequestProperties finalClientRequestProperties = clientRequestProperties;
            return streamMono.flatMap(stream -> {
                log.debug("Executing streaming ingest");
                return Mono.fromCallable(() -> this.streamingClient.executeStreamingIngest( //TODO: replace with async equivalent
                                ingestionProperties.getDatabaseName(),
                                ingestionProperties.getTableName(),
                                stream,
                                finalClientRequestProperties,
                                dataFormat.getKustoValue(),
                                ingestionProperties.getIngestionMapping().getIngestionMappingReference(),
                                !(streamSourceInfo.getCompressionType() == null || !streamSourceInfo.isLeaveOpen())))
                        .doOnTerminate(() -> log.debug("Stream was ingested successfully."))
                        .map(ignored -> {
                            IngestionStatus ingestionStatus = new IngestionStatus();
                            ingestionStatus.status = OperationStatus.Succeeded;
                            ingestionStatus.table = ingestionProperties.getTableName();
                            ingestionStatus.database = ingestionProperties.getDatabaseName();
                            return (IngestionResult) new IngestionStatusResult(ingestionStatus);
                        });
            }).onErrorMap(DataClientException.class, e -> {
                String msg = ExceptionsUtils.getMessageEx(e);
                log.error(msg, e);
                return new IngestionClientException(msg, e);
            }).onErrorMap(IOException.class, e -> {
                String msg = ExceptionsUtils.getMessageEx(e);
                log.error(msg, e);
                return new IngestionClientException(msg, e);
            }).onErrorMap(DataServiceException.class, e -> {
                log.error(e.getMessage(), e);
                return new IngestionServiceException(e.getMessage(), e);
            });
        } catch (Exception e) {
            return Mono.error(e);
        }
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

    IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo,
                                   IngestionProperties ingestionProperties,
                                   BlobClient cloudBlockBlob,
                                   @Nullable String clientRequestId)
            throws IngestionClientException, IngestionServiceException {
        // trace ingestFromBlob
        return MonitoredActivity.invoke(
                (SupplierTwoExceptions<IngestionResult, IngestionClientException, IngestionServiceException>) () -> ingestFromBlobImpl(blobSourceInfo,
                        ingestionProperties, cloudBlockBlob, clientRequestId),
                getClientType().concat(".ingestFromBlob"),
                getIngestionTraceAttributes(blobSourceInfo, ingestionProperties));
    }

    private IngestionResult ingestFromBlobImpl(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties, BlobClient cloudBlockBlob,
                                               @Nullable String clientRequestId)
            throws IngestionClientException, IngestionServiceException {
        String blobPath = blobSourceInfo.getBlobPath();
        try {
            // No need to check blob size if it was given to us that it's not empty
            if (blobSourceInfo.getRawSizeInBytes() == 0 && cloudBlockBlob.getProperties().getBlobSize() == 0) {
                String message = "Empty blob.";
                log.error(message);
                throw new IngestionClientException(message);
            }
        } catch (BlobStorageException ex) {
            throw new IngestionClientException(String.format("Exception trying to read blob metadata,%s",
                    ex.getStatusCode() == HttpStatus.FORBIDDEN ? "this might mean the blob doesn't exist" : ""), ex);
        }
        ClientRequestProperties clientRequestProperties = null;
        if (StringUtils.isNotBlank(clientRequestId)) {
            clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setClientRequestId(clientRequestId);
        }
        IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();
        try {
            this.streamingClient.executeStreamingIngestFromBlob(ingestionProperties.getDatabaseName(),
                    ingestionProperties.getTableName(),
                    blobPath,
                    clientRequestProperties,
                    dataFormat.getKustoValue(),
                    ingestionProperties.getIngestionMapping().getIngestionMappingReference());
        } catch (DataClientException e) {
            log.error(e.getMessage(), e);
            throw new IngestionClientException(e.getMessage(), e);
        } catch (DataServiceException e) {
            log.error(e.getMessage(), e);
            throw new IngestionServiceException(e.getMessage(), e);
        }

        log.debug("Blob was ingested successfully.");
        IngestionStatus ingestionStatus = new IngestionStatus();
        ingestionStatus.status = OperationStatus.Succeeded;
        ingestionStatus.table = ingestionProperties.getTableName();
        ingestionStatus.database = ingestionProperties.getDatabaseName();
        return new IngestionStatusResult(ingestionStatus);
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
        String blobPath = blobSourceInfo.getBlobPath();
        try {
            // No need to check blob size if it was given to us that it's not empty
            if (blobSourceInfo.getRawSizeInBytes() == 0 && cloudBlockBlob.getProperties().getBlobSize() == 0) { //the getProperties() is a synchronous blocking call
                String message = "Empty blob.";
                log.error(message);
                return Mono.error(new IngestionClientException(message));
            }
        } catch (BlobStorageException ex) {
            return Mono.error(new IngestionClientException(String.format("Exception trying to read blob metadata,%s",
                    ex.getStatusCode() == HttpStatus.FORBIDDEN ? "this might mean the blob doesn't exist" : ""), ex));
        }

        ClientRequestProperties clientRequestProperties = null;
        if (StringUtils.isNotBlank(clientRequestId)) {
            clientRequestProperties = new ClientRequestProperties();
            clientRequestProperties.setClientRequestId(clientRequestId);
        }
        IngestionProperties.DataFormat dataFormat = ingestionProperties.getDataFormat();

        ClientRequestProperties finalClientRequestProperties = clientRequestProperties;
        return Mono.fromCallable(() -> this.streamingClient.executeStreamingIngestFromBlob(ingestionProperties.getDatabaseName(), //TODO: replace with async equivalent
                        ingestionProperties.getTableName(),
                        blobPath,
                        finalClientRequestProperties,
                        dataFormat.getKustoValue(),
                        ingestionProperties.getIngestionMapping().getIngestionMappingReference()))
                .onErrorMap(DataClientException.class, e -> {
                    log.error(e.getMessage(), e);
                    return new IngestionClientException(e.getMessage(), e);
                })
                .onErrorMap(DataServiceException.class, e -> {
                    log.error(e.getMessage(), e);
                    return new IngestionServiceException(e.getMessage(), e);
                })
                .doOnTerminate(() -> log.debug("Blob was ingested successfully."))
                .map(ignored -> {
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
