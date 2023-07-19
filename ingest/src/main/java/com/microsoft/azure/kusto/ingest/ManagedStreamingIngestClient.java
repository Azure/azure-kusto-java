package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
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
import com.microsoft.azure.kusto.ingest.source.*;

import com.microsoft.azure.kusto.ingest.utils.ExponentialRetry;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.sql.Blob;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * <p>ManagedStreamingIngestClient</p>
 * <p>
 * This class combines a managed streaming client with a queued streaming client, to create an optimized experience.
 * Since the streaming client communicates directly with the engine, it's more prone to failure, so this class
 * holds both a streaming client and a queued client.
 * It tries {@value ATTEMPT_COUNT} times using the streaming client, after which it falls back to the queued streaming client in case of failure.
 * If the size of the stream is bigger than {@value MAX_STREAMING_SIZE_BYTES}, it will fall back to the queued streaming client.
 * <p>
 */
public class ManagedStreamingIngestClient implements IngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final int ATTEMPT_COUNT = 3;
    public static final int MAX_STREAMING_SIZE_BYTES = 4 * 1024 * 1024;
    final QueuedIngestClientImpl queuedIngestClient;
    final StreamingIngestClient streamingIngestClient;
    private final ExponentialRetry exponentialRetryTemplate;
    private CloseableHttpClient httpClient = null;

    /**
     * @param dmConnectionString dm connection string
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from a DM connection string, with default http client properties.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString) throws URISyntaxException {
        return fromDmConnectionString(dmConnectionString, (HttpClientProperties) null, true);
    }

    /**
     * @param dmConnectionString dm connection string
     * @param properties         additional properties to configure the http client
     * @param autoCorrectEndpoint Flag to indicate whether to correct the endpoint URI or not
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from a DM connection string.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString,
            @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint)
            throws URISyntaxException {
        ConnectionStringBuilder engineConnectionString = new ConnectionStringBuilder(dmConnectionString);
        engineConnectionString.setClusterUrl(IngestClientBase.getQueryEndpoint(engineConnectionString.getClusterUrl()));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties, autoCorrectEndpoint);
    }

    /**
     * @param engineConnectionString engine connection string
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from an engine connection string, with default http client properties.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString) throws URISyntaxException {
        return fromEngineConnectionString(engineConnectionString, null, true);
    }

    /**
     * @param engineConnectionString engine connection string
     * @param properties             additional properties to configure the http client
     * @param autoCorrectEndpoint Flag to indicate whether to correct the endpoint URI or not
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from an engine connection string.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString,
            @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint)
            throws URISyntaxException {
        ConnectionStringBuilder dmConnectionString = new ConnectionStringBuilder(engineConnectionString);
        dmConnectionString.setClusterUrl(IngestClientBase.getIngestionEndpoint(engineConnectionString.getClusterUrl()));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties, autoCorrectEndpoint);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - This method is slated to be private. Use
     * {@link IngestClientFactory#createManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     * instead.
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder) throws URISyntaxException {
        this(ingestionEndpointConnectionStringBuilder, queryEndpointConnectionStringBuilder, null, true);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param autoCorrectEndpoint - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder, boolean autoCorrectEndpoint) throws URISyntaxException {
        this(ingestionEndpointConnectionStringBuilder, queryEndpointConnectionStringBuilder, null, autoCorrectEndpoint);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param properties                               - Additional properties to configure the http client
     * @param autoCorrectEndpoint - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - This method is slated to be private.  Use
     * {@link IngestClientFactory#createManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder, HttpClientProperties)} instead.
     * This constructor should only be used for advanced cases. If your endpoints are standard, or you do not know, use
     * {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)})} instead.
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder,
            @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(ingestionEndpointConnectionStringBuilder, properties, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(queryEndpointConnectionStringBuilder, properties, autoCorrectEndpoint);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param connectionStringBuilder - Client connection string
     * @param properties - Additional properties to configure the http client
     * @param autoCorrectEndpoint - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, properties, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, properties, autoCorrectEndpoint);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param connectionStringBuilder - Client connection string
     * @param httpClient - HTTP client
     * @param autoCorrectEndpoint - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable CloseableHttpClient httpClient, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, httpClient, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, httpClient);
        this.httpClient = httpClient;
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder - Endpoint for querying data, does not include "ingest-"
     * @param properties                               - Additional properties to configure the http client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
            ConnectionStringBuilder queryEndpointConnectionStringBuilder,
            @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(ingestionEndpointConnectionStringBuilder, properties, true);
        streamingIngestClient = new StreamingIngestClient(queryEndpointConnectionStringBuilder, properties, true);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /***
     * @param connectionStringBuilder - Client connection string
     * @param properties - Additional properties to configure the http client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, properties, true);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, properties, true);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /***
     * @param connectionStringBuilder - Client connection string
     * @param httpClient - HTTP client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
            @Nullable CloseableHttpClient httpClient) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, httpClient, true);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, httpClient);
        this.httpClient = httpClient;
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param resourceManager ingestion resources manager
     * @param storageClient   - storage utilities
     * @param streamingClient - the streaming client
     * @deprecated - This method is slated to be private. Use
     * {@link IngestClientFactory#createManagedStreamingIngestClient(ConnectionStringBuilder)} instead.
     */
    public ManagedStreamingIngestClient(ResourceManager resourceManager,
            AzureStorageClient storageClient,
            StreamingClient streamingClient) {
        log.info("Creating a new ManagedStreamingIngestClient from raw parts");
        queuedIngestClient = new QueuedIngestClientImpl(resourceManager, storageClient);
        streamingIngestClient = new StreamingIngestClient(streamingClient);
        exponentialRetryTemplate = new ExponentialRetry(ATTEMPT_COUNT);
    }

    /**
     * @param resourceManager ingestion resources manager
     * @param storageClient - storage utilities
     * @param streamingClient - the streaming client
     * @param retryTemplate - retry template
     */
    public ManagedStreamingIngestClient(ResourceManager resourceManager,
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
     */
    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        BlobClientBuilder blobClientBuilder = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath());
        if (httpClient != null) {
            blobClientBuilder.httpClient((HttpClient) httpClient);
        }

        BlobClient blobClient = blobClientBuilder.buildClient();
        if (blobSourceInfo.getRawSizeInBytes() <= 0) {
            try {
                blobSourceInfo.setRawSizeInBytes(blobClient.getProperties().getBlobSize());
            } catch (BlobStorageException e) {
                throw new IngestionServiceException(
                        blobSourceInfo.getBlobPath(),
                        "Failed getting blob properties: " + e.getMessage(),
                        e);
            }
        }

        if (blobSourceInfo.getRawSizeInBytes() > MAX_STREAMING_SIZE_BYTES) {
            log.info("Blob size is greater than max streaming size ({} bytes). Falling back to queued.", blobSourceInfo.getRawSizeInBytes());
            return queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
        }

        IngestionResult result = streamWithRetries(blobSourceInfo, ingestionProperties, blobClient);
        if (result != null) {
            return result;
        }
        return queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
    }

    private IngestionResult streamWithRetries(SourceInfo sourceInfo, IngestionProperties ingestionProperties, @Nullable BlobClient blobClient)
            throws IngestionClientException, IngestionServiceException {
        ExponentialRetry retry = new ExponentialRetry(exponentialRetryTemplate);
        return retry.execute(currentAttempt -> {
            try {
                if (blobClient != null) {
                    String clientRequestId = String.format("KJC.executeManagedStreamingIngest.ingestFromBlob;%s;%d", sourceInfo.getSourceId(), currentAttempt);
                    return streamingIngestClient.ingestFromBlob((BlobSourceInfo) sourceInfo, ingestionProperties, blobClient, clientRequestId);
                } else {
                    String clientRequestId = String.format("KJC.executeManagedStreamingIngest.ingestFromStream;%s;%d", sourceInfo.getSourceId(),
                            currentAttempt);
                    return streamingIngestClient.ingestFromStream((StreamSourceInfo) sourceInfo, ingestionProperties, clientRequestId);
                }
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

                if (sourceInfo instanceof StreamSourceInfo) {
                    try {
                        ((StreamSourceInfo) sourceInfo).getStream().reset();
                    } catch (IOException ioException) {
                        throw new IngestionClientException("Failed to reset stream", ioException);
                    }
                }

            }
            return null;
        });
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
        try {
            IngestionResult result = streamWithRetries(managedSourceInfo, ingestionProperties, null);
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
    public void close() throws IOException {
        queuedIngestClient.close();
        streamingIngestClient.close();
    }
}
