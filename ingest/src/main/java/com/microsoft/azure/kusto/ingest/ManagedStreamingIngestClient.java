package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import com.microsoft.azure.kusto.data.exceptions.DataWebException;
import com.microsoft.azure.kusto.data.exceptions.OneApiError;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * <p>ManagedStreamingIngestClient</p>
 * <p>
 * This class combines a managed streaming client with a queued streaming client, to create an optimized experience.
 * Since the streaming client communicates directly with the engine, it's more prone to failure, so this class
 * holds both a streaming client and a queued client.
 * It tries {@value ATTEMPT_COUNT} times using the streaming client, after which it falls back to the queued streaming client in case of failure.
 * If the size of the stream is bigger than {@value MAX_STREAMING_SIZE_BYTES}, it will fall back to the queued streaming client.
 * <p>
 * Note that {@code ingestFromBlob} behaves differently from the other methods - since a blob already exists it makes more sense to enqueue it rather than downloading and streaming it, thus ManagedStreamingIngestClient skips the streaming retries and sends it directly to the queued client.
 */
public class ManagedStreamingIngestClient implements IngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final int ATTEMPT_COUNT = 3;
    public static final int MAX_STREAMING_SIZE_BYTES = 4 * 1024 * 1024;
    final QueuedIngestClientImpl queuedIngestClient;
    final StreamingIngestClient streamingIngestClient;
    private final ExponentialRetry exponentialRetryTemplate;

    /**
     * Creates a new ManagedStreamingIngestClient from a DM connection string, with default http client properties.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     * @param dmConnectionString dm connection string
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString) throws URISyntaxException {
        return fromDmConnectionString(dmConnectionString, null);
    }

    /**
     * Creates a new ManagedStreamingIngestClient from a DM connection string.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     * @param dmConnectionString dm connection string
     * @param properties additional properties to configure the http client
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString,
            @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        ConnectionStringBuilder engineConnectionString = new ConnectionStringBuilder(dmConnectionString);
        engineConnectionString.setClusterUrl(StreamingIngestClient.generateEngineUriSuggestion(new URIBuilder(dmConnectionString.getClusterUrl())));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties);
    }

    /**
     * Creates a new ManagedStreamingIngestClient from an engine connection string, with default http client properties.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     * @param engineConnectionString engine connection string
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString) throws URISyntaxException {
        return fromEngineConnectionString(engineConnectionString, null);
    }

    /**
     * Creates a new ManagedStreamingIngestClient from an engine connection string.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     * @param engineConnectionString engine connection string
     * @param properties additional properties to configure the http client
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString,
            @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        ConnectionStringBuilder dmConnectionString = new ConnectionStringBuilder(engineConnectionString);
        dmConnectionString.setClusterUrl(QueuedIngestClientImpl.generateDmUriSuggestion(new URIBuilder(engineConnectionString.getClusterUrl())));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties);
    }

    public ManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
            ConnectionStringBuilder engineConnectionStringBuilder) throws URISyntaxException {
        this(dmConnectionStringBuilder, engineConnectionStringBuilder, null);
    }

    public ManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
            ConnectionStringBuilder engineConnectionStringBuilder,
            @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(dmConnectionStringBuilder, properties);
        streamingIngestClient = new StreamingIngestClient(engineConnectionStringBuilder, properties);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    public ManagedStreamingIngestClient(ResourceManager resourceManager,
            AzureStorageClient storageClient,
            StreamingClient streamingClient) {
        log.info("Creating a new ManagedStreamingIngestClient from raw parts");
        queuedIngestClient = new QueuedIngestClientImpl(resourceManager, storageClient);
        streamingIngestClient = new StreamingIngestClient(streamingClient);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    ManagedStreamingIngestClient(ResourceManager resourceManager,
            AzureStorageClient storageClient,
            StreamingClient streamingClient,
            ExponentialRetry retryTemplate) {
        log.info("Creating a new ManagedStreamingIngestClient from raw parts");
        queuedIngestClient = new QueuedIngestClientImpl(resourceManager, storageClient);
        streamingIngestClient = new StreamingIngestClient(streamingClient);
        exponentialRetryTemplate = retryTemplate;
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();
        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, true);
            return ingestFromStream(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
            log.error("File not found when ingesting a file.", e);
            throw new IngestionClientException("IO exception - check file path.", e);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method behaves differently from the rest for {@link ManagedStreamingIngestClient} - since a blob already exists it makes more sense to enqueue it rather than downloading and streaming it, thus ManagedStreamingIngestClient skips the streaming retries and sends it directly to the queued client.</p>
     */
    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        // If it's a blob we ingest using the queued client
        return queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
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
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        UUID sourceId = streamSourceInfo.getSourceId();
        if (sourceId == null) {
            sourceId = UUID.randomUUID();
        }

        byte[] streamingBytes;
        try {
            streamingBytes = IngestionUtils.readBytesFromInputStream(streamSourceInfo.getStream(), MAX_STREAMING_SIZE_BYTES + 1);
        } catch (IOException e) {
            throw new IngestionClientException("Failed to read from stream.", e);
        }

        // ByteArrayInputStream's close method is a no-op, so we don't need to close it.
        ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(streamingBytes);

        if (streamingBytes.length > MAX_STREAMING_SIZE_BYTES) {
            log.info("Stream size is greater than max streaming size ({} bytes). Falling back to queued.", streamingBytes.length);
            StreamSourceInfo managedSourceInfo = new StreamSourceInfo(new SequenceInputStream(byteArrayStream, streamSourceInfo.getStream()),
                    streamSourceInfo.isLeaveOpen(), sourceId, streamSourceInfo.getCompressionType());
            return queuedIngestClient.ingestFromStream(managedSourceInfo, ingestionProperties);
        }

        if (!streamSourceInfo.isLeaveOpen()) {
            // From this point we don't need the original stream anymore, we cached it
            try {
                streamSourceInfo.getStream().close();
            } catch (IOException e) {
                log.warn("Failed to close stream", e);
            }
        }

        StreamSourceInfo managedSourceInfo = new StreamSourceInfo(byteArrayStream, true, sourceId, streamSourceInfo.getCompressionType());

        ExponentialRetry retry = new ExponentialRetry(exponentialRetryTemplate);

        UUID finalSourceId = sourceId;
        try {
            IngestionResult result = retry.execute(currentAttempt -> {
                try {
                    String clientRequestId = String.format("KJC.executeManagedStreamingIngest;%s;%d", finalSourceId, currentAttempt);
                    return streamingIngestClient.ingestFromStream(managedSourceInfo, ingestionProperties, clientRequestId);
                } catch (Exception e) {
                    if (e instanceof IngestionServiceException
                            && e.getCause() != null
                            && e.getCause() instanceof DataServiceException
                            && e.getCause().getCause() != null
                            && e.getCause().getCause() instanceof DataWebException) {
                        DataWebException webException = (DataWebException) e.getCause().getCause();
                        OneApiError oneApiError = webException.getApiError();
                        if (oneApiError.isPermanent()) {
                            throw e;
                        }
                    }

                    log.info(String.format("Streaming ingestion failed attempt %d", currentAttempt), e);

                    try {
                        managedSourceInfo.getStream().reset();
                    } catch (IOException ioException) {
                        throw new IngestionClientException("Failed to reset stream", ioException);
                    }
                }
                return null;
            });

            if (result != null) {
                return result;
            }

            return queuedIngestClient.ingestFromStream(managedSourceInfo, ingestionProperties);
        } finally {
            try {
                managedSourceInfo.getStream().close();
            } catch (IOException e) {
                log.warn("Failed to close byte stream", e);
            }
        }
    }

    @Override
    public void close() {
        queuedIngestClient.close();
        streamingIngestClient.close();
    }
}
