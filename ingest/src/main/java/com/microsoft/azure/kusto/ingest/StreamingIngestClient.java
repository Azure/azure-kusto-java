// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.microsoft.azure.kusto.data.*;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
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
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.zip.GZIPOutputStream;

public class StreamingIngestClient extends IngestClientBase implements IngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int STREAM_COMPRESS_BUFFER_SIZE = 16 * 1024;
    private final StreamingClient streamingClient;

    StreamingIngestClient(ConnectionStringBuilder csb, @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new StreamingIngestClient");
        ConnectionStringBuilder csbWithEndpoint = new ConnectionStringBuilder(csb);
        csbWithEndpoint.setClusterUrl(getQueryEndpoint(csbWithEndpoint.getClusterUrl()));
        this.streamingClient = ClientFactory.createStreamingClient(csbWithEndpoint, properties);
        this.connectionDataSource = csbWithEndpoint.getClusterUrl();
    }

    StreamingIngestClient(ConnectionStringBuilder csb, @Nullable CloseableHttpClient httpClient) throws URISyntaxException {
        log.info("Creating a new StreamingIngestClient");
        this.streamingClient = ClientFactory.createStreamingClient(csb, httpClient);
        this.connectionDataSource = csb.getClusterUrl();
    }

    StreamingIngestClient(StreamingClient streamingClient) {
        log.info("Creating a new StreamingIngestClient");
        this.streamingClient = streamingClient;
    }

    public static String generateEngineUriSuggestion(URIBuilder existingEndpoint) {
        if (!existingEndpoint.getHost().toLowerCase().startsWith(IngestClientBase.INGEST_PREFIX)) {
            throw new IllegalArgumentException("The URL is already formatted as the suggested Engine endpoint, so no suggestion can be made");
        }

        existingEndpoint.setHost(existingEndpoint.getHost().substring(IngestClientBase.INGEST_PREFIX.length()));
        return existingEndpoint.toString();
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, false);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        log.warn("Ingesting from blob using the StreamingIngestClient is not recommended, consider using the IngestClient instead.");
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        try {
            BlobClient blobClient = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath()).buildClient();
            return ingestFromBlob(blobSourceInfo, ingestionProperties, blobClient);
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
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        // Argument validation:
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
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        return ingestFromStream(streamSourceInfo, ingestionProperties, null);
    }

    IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties, @Nullable String clientRequestId)
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
            log.error(e.getMessage(), e);
            throw new IngestionClientException(e.getMessage(), e);
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

    IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties, BlobClient cloudBlockBlob)
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
                    ex.getStatusCode() == 403 ? "this might mean the blob doesn't exist" : ""), ex);
        }
        InputStream stream = cloudBlockBlob.openInputStream();
        StreamSourceInfo streamSourceInfo = new StreamSourceInfo(stream, false, blobSourceInfo.getSourceId(), IngestionUtils.getCompression(blobPath));
        return ingestFromStream(streamSourceInfo, ingestionProperties);
    }

    protected void setConnectionDataSource(String connectionDataSource) {
        this.connectionDataSource = connectionDataSource;
    }

    @Override
    public void close() {
    }
}
