package com.microsoft.azure.kusto.ingest;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.kusto.data.BaseClient;
import com.microsoft.azure.kusto.data.Ensure;
import com.microsoft.azure.kusto.data.ExponentialRetry;
import com.microsoft.azure.kusto.data.StreamingClient;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.exceptions.*;
import com.microsoft.azure.kusto.data.http.HttpClientProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.SourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.kusto.ingest.utils.IngestionUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.UUID;

/**
 * <p>ManagedStreamingIngestClient</p>
 * <p>
 * This class combines a managed streaming client with a queued streaming client, to create an optimized experience.
 * Since the streaming client communicates directly with the engine, it's more prone to failure, so this class
 * holds both a streaming client and a queued client.
 * It retries {@value RETRY_COUNT} times using the streaming client, after which it falls back to the queued streaming client in case of failure.
 * By default the policy for choosing a queued ingestion on the first try is the checking of weather the size of the estimated
 * raw stream size (a conversion to compressed CSV) is bigger than 4MB, it will fall back to the queued streaming client.
 * Use {@link #setQueuingPolicyFactor(double)} to override the predicate heuristics.
 * Use SourceInfo.setRawSizeInBytes to set the raw size of the data.
 */
public class ManagedStreamingIngestClient extends IngestClientBase implements QueuedIngestClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    // 2 Retry count ends with total 3 streaming attempts
    public static final int RETRY_COUNT = 2;
    public static final String CLASS_NAME = ManagedStreamingIngestClient.class.getSimpleName();
    final QueuedIngestClient queuedIngestClient;
    final StreamingIngestClient streamingIngestClient;
    private ExponentialRetry exponentialRetryTemplate = new ExponentialRetry(RETRY_COUNT);
    private Retry streamingRetry = new ExponentialRetry(exponentialRetryTemplate).retry(null, this::streamingIngestionErrorPredicate);
    private HttpClient httpClient = null;
    private ManagedStreamingQueuingPolicy queuingPolicy = ManagedStreamingQueuingPolicy.Default;
    private static final String FALLBACK_LOG_STRING = "Data size for source id '%s' is greater than max streaming size according to the policy. Falling back to queued.";

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
        return fromDmConnectionString(dmConnectionString, (HttpClientProperties) null);
    }

    /**
     * @param dmConnectionString dm connection string
     * @param properties         additional properties to configure the http client
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from a DM connection string.
     * This method infers the engine connection string from the DM connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromDmConnectionString(ConnectionStringBuilder dmConnectionString,
                                                                      @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        ConnectionStringBuilder engineConnectionString = new ConnectionStringBuilder(dmConnectionString);
        engineConnectionString.setClusterUrl(IngestClientBase.getQueryEndpoint(engineConnectionString.getClusterUrl()));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties);
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
        return fromEngineConnectionString(engineConnectionString, null);
    }

    /**
     * @param engineConnectionString engine connection string
     * @param properties             additional properties to configure the http client
     * @return a new ManagedStreamingIngestClient
     * @throws URISyntaxException if the connection string is invalid
     * @deprecated - Ingest clients now automatically deduce the endpoint, use {@link #ManagedStreamingIngestClient(ConnectionStringBuilder, HttpClientProperties)} instead.
     * Creates a new ManagedStreamingIngestClient from an engine connection string.
     * This method infers the DM connection string from the engine connection string.
     * For advanced usage, use {@link ManagedStreamingIngestClient#ManagedStreamingIngestClient(ConnectionStringBuilder, ConnectionStringBuilder)}
     */
    public static ManagedStreamingIngestClient fromEngineConnectionString(ConnectionStringBuilder engineConnectionString,
                                                                          @Nullable HttpClientProperties properties)
            throws URISyntaxException {
        ConnectionStringBuilder dmConnectionString = new ConnectionStringBuilder(engineConnectionString);
        dmConnectionString.setClusterUrl(IngestClientBase.getIngestionEndpoint(engineConnectionString.getClusterUrl()));
        return new ManagedStreamingIngestClient(dmConnectionString, engineConnectionString, properties);
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
        this(ingestionEndpointConnectionStringBuilder, queryEndpointConnectionStringBuilder, null);
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param autoCorrectEndpoint                      - Flag to indicate whether to correct the endpoint URI or not
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
    }

    /**
     * @param connectionStringBuilder - Client connection string
     * @param properties              - Additional properties to configure the http client
     * @param autoCorrectEndpoint     - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
                                        @Nullable HttpClientProperties properties, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, properties, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, properties, autoCorrectEndpoint);
    }

    /**
     * @param connectionStringBuilder - Client connection string
     * @param httpClient              - HTTP client
     * @param autoCorrectEndpoint     - Flag to indicate whether to correct the endpoint URI or not
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
                                        @Nullable HttpClient httpClient, boolean autoCorrectEndpoint) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, httpClient, autoCorrectEndpoint);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, httpClient, autoCorrectEndpoint);
        this.httpClient = httpClient;
    }

    /**
     * @param ingestionEndpointConnectionStringBuilder - Endpoint for ingesting data, usually starts with "https://ingest-"
     * @param queryEndpointConnectionStringBuilder     - Endpoint for querying data, does not include "ingest-"
     * @param properties                               - Additional properties to configure the http client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder ingestionEndpointConnectionStringBuilder,
                                        ConnectionStringBuilder queryEndpointConnectionStringBuilder,
                                        @Nullable HttpClientProperties properties) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(ingestionEndpointConnectionStringBuilder, properties, true);
        streamingIngestClient = new StreamingIngestClient(queryEndpointConnectionStringBuilder, properties, true);
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
    }

    /***
     * @param connectionStringBuilder - Client connection string
     * @param httpClient - HTTP client
     * @throws URISyntaxException if the connection string is invalid
     */
    public ManagedStreamingIngestClient(ConnectionStringBuilder connectionStringBuilder,
                                        @Nullable HttpClient httpClient) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClientImpl(connectionStringBuilder, httpClient, true);
        streamingIngestClient = new StreamingIngestClient(connectionStringBuilder, httpClient, true);
        this.httpClient = httpClient;
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
    }

    /**
     * @param resourceManager ingestion resources manager
     * @param storageClient   - storage utilities
     * @param streamingClient - the streaming client
     * @param retryTemplate   - retry template
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

    ManagedStreamingIngestClient(StreamingIngestClient streamingIngestClient, QueuedIngestClient queuedIngestClient, ExponentialRetry exponentialRetry) {
        this.streamingIngestClient = streamingIngestClient;
        this.queuedIngestClient = queuedIngestClient;
        streamingRetry = new ExponentialRetry(exponentialRetry).retry(null, this::streamingIngestionErrorPredicate);
    }

    @Override
    protected Mono<IngestionResult> ingestFromFileAsyncImpl(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        fileSourceInfo.validate();
        ingestionProperties.validate();

        try {
            StreamSourceInfo streamSourceInfo = IngestionUtils.fileToStream(fileSourceInfo, true);
            return ingestFromStreamAsync(streamSourceInfo, ingestionProperties);
        } catch (FileNotFoundException e) {
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

        BlobClientBuilder blobClientBuilder = new BlobClientBuilder().endpoint(blobSourceInfo.getBlobPath());
        if (httpClient != null) {
            blobClientBuilder.httpClient(httpClient);
        }
        BlobAsyncClient blobAsyncClient = blobClientBuilder.buildAsyncClient();

        return (blobSourceInfo.getBlobExactSize() > 0
                ? Mono.just(blobSourceInfo.getBlobExactSize())
                : blobAsyncClient.getProperties().map(BlobProperties::getBlobSize))
                .onErrorMap(BlobStorageException.class, e -> new IngestionServiceException(
                        blobSourceInfo.getBlobPath(),
                        "Failed getting blob properties: " + ExceptionUtils.getMessageEx(e),
                        e))
                .flatMap(blobSize -> handleBlobIngestion(blobSourceInfo, ingestionProperties, blobAsyncClient, blobSize));
    }


    private Mono<IngestionResult> handleBlobIngestion(BlobSourceInfo blobSourceInfo,
                                                      IngestionProperties ingestionProperties,
                                                      BlobAsyncClient blobAsyncClient,
                                                      long blobSize) {
        if (queuingPolicy.shouldUseQueuedIngestion(blobSize, blobSourceInfo.getCompressionType() != null, ingestionProperties.getDataFormat())) {
            log.info(String.format(FALLBACK_LOG_STRING, blobSourceInfo.getSourceId()));
            return queuedIngestClient.ingestFromBlobAsync(blobSourceInfo, ingestionProperties);
        }
        IngestionUtils.IntegerHolder i = new IngestionUtils.IntegerHolder();

        // If an error occurs, each time the retryWhen subscribes to executeStream create a new instance
        // instead of using the same executeStream Mono for all retries
        return executeStream(blobSourceInfo, ingestionProperties, blobAsyncClient, i.increment())
                .retryWhen(streamingRetry)
                .onErrorResume(e -> {
                    if (streamingIngestionErrorPredicate(e)) {
                        log.info("Streaming ingestion failed for sourceId: {}, falling back to queued ingestion.", blobSourceInfo.getSourceId());
                        return queuedIngestClient.ingestFromBlobAsync(blobSourceInfo, ingestionProperties);
                    }
                    return Mono.error(e);
                }); // Fall back to queued ingestion
    }

    private void resetStream(StreamSourceInfo sourceInfo) {
        try {
            sourceInfo.getStream().reset();
        } catch (IOException ioException) {
            throw new IngestionClientException("Failed to reset stream for retry", ioException);
        }
    }

    private Mono<IngestionResult> executeStream(SourceInfo sourceInfo, IngestionProperties ingestionProperties, @Nullable BlobAsyncClient blobAsyncClient, int currentAttempt) {
        if (blobAsyncClient != null) {
            String clientRequestId = String.format("KJC.executeManagedStreamingIngest.ingestFromBlob;%s;%d", sourceInfo.getSourceId(), currentAttempt);
            return streamingIngestClient.ingestFromBlobAsync((BlobSourceInfo) sourceInfo, ingestionProperties, clientRequestId);
        }

        String clientRequestId = String.format("KJC.executeManagedStreamingIngest.ingestFromStream;%s;%d", sourceInfo.getSourceId(), currentAttempt);
        return streamingIngestClient.ingestFromStreamAsync((StreamSourceInfo) sourceInfo, ingestionProperties, clientRequestId);
    }

    private boolean streamingIngestionErrorPredicate(Throwable e) {
        if (e instanceof IngestionServiceException
                && e.getCause() != null
                && e.getCause() instanceof DataServiceException
                && e.getCause().getCause() != null
                && e.getCause().getCause() instanceof DataWebException) {
            DataWebException webException = (DataWebException) e.getCause().getCause();
            OneApiError oneApiError = webException.getApiError();
            if (oneApiError.isPermanent()) {
                return false;
            }
        }

        log.info("Streaming ingestion failed.", e);

        return true;
    }

    @Override
    protected Mono<IngestionResult> ingestFromResultSetAsyncImpl(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        resultSetSourceInfo.validate();
        ingestionProperties.validateResultSetProperties();
        return Mono.fromCallable(() -> IngestionUtils.resultSetToStream(resultSetSourceInfo))// TODO: ?
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorMap(IOException.class, e -> {
                    String msg = "Failed to read from ResultSet.";
                    log.error(msg, e);
                    return new IngestionClientException(msg, e);
                })
                .flatMap(streamSourceInfo -> ingestFromStreamAsync(streamSourceInfo, ingestionProperties));
    }

    @Override
    protected Mono<IngestionResult> ingestFromStreamAsyncImpl(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");
        streamSourceInfo.validate();
        ingestionProperties.validate();

        if (streamSourceInfo.getSourceId() == null) {
            streamSourceInfo.setSourceId(UUID.randomUUID());
        }

        try {
            int availableBytes = streamSourceInfo.getStream().available();
            boolean shouldUseQueuedIngestion = queuingPolicy.shouldUseQueuedIngestion(
                    availableBytes,
                    streamSourceInfo.getCompressionType() != null,
                    ingestionProperties.getDataFormat());
            return shouldUseQueuedIngestion
                    ? queuedIngestClient.ingestFromStreamAsync(streamSourceInfo, ingestionProperties)
                    : processStream(streamSourceInfo, ingestionProperties);
        } catch (IOException e) {
            throw new IngestionClientException("Failed to read from stream.", e);
        }
    }

    private Mono<IngestionResult> ingestStreamWithRetries(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {
        IngestionUtils.IntegerHolder integerHolder = new IngestionUtils.IntegerHolder();
        return Mono.defer(() -> executeStream(streamSourceInfo, ingestionProperties, null, integerHolder.increment()))
                .doOnError((e) -> resetStream(streamSourceInfo))
                .retryWhen(streamingRetry)
                .onErrorResume(e -> {
                    // If the error is not recoverable, we should not fall back to queued ingestion
                    if (streamingIngestionErrorPredicate(e)) {
                        log.info("Streaming ingestion failed for sourceId: {}, falling back to queued ingestion.", streamSourceInfo.getSourceId());
                        return queuedIngestClient.ingestFromStreamAsync(streamSourceInfo, ingestionProperties);
                    }

                    return Mono.error(e);
                })
                .doFinally(signal -> closeStreamSafely(streamSourceInfo));
    }

    private Mono<IngestionResult> processStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) {

        if (streamSourceInfo.getStream() instanceof ByteArrayInputStream || streamSourceInfo.getStream() instanceof ResettableFileInputStream) {
            StreamSourceInfo managedSourceInfo = new StreamSourceInfo(streamSourceInfo.getStream(),
                    true, streamSourceInfo.getSourceId(), streamSourceInfo.getCompressionType());
            return ingestStreamWithRetries(managedSourceInfo, ingestionProperties);
        }
        try {
            byte[] streamingBytes = IngestionUtils.readBytesFromInputStream(streamSourceInfo.getStream(),
                    ManagedStreamingQueuingPolicy.MAX_STREAMING_STREAM_SIZE_BYTES + 1);

            InputStream byteArrayStream = new ByteArrayInputStream(streamingBytes);
            int size = streamingBytes.length;

            boolean shouldUseQueuedIngestion = queuingPolicy.shouldUseQueuedIngestion(
                    size,
                    streamSourceInfo.getCompressionType() != null,
                    ingestionProperties.getDataFormat());

            if (shouldUseQueuedIngestion) {
                log.info(String.format(FALLBACK_LOG_STRING, streamSourceInfo.getSourceId()));
                StreamSourceInfo managedSourceInfo = new StreamSourceInfo(new SequenceInputStream(byteArrayStream, streamSourceInfo.getStream()),
                        streamSourceInfo.isLeaveOpen(), streamSourceInfo.getSourceId(), streamSourceInfo.getCompressionType());

                return queuedIngestClient.ingestFromStreamAsync(managedSourceInfo, ingestionProperties);
            }

            if (!streamSourceInfo.isLeaveOpen()) {

                // From this point we don't need the original stream anymore, we cached it
                try {
                    streamSourceInfo.getStream().close();
                } catch (IOException e) {
                    log.warn("Failed to close stream", e);
                }
            }

            StreamSourceInfo managedSourceInfo = new StreamSourceInfo(byteArrayStream,
                    true, streamSourceInfo.getSourceId(), streamSourceInfo.getCompressionType());
            return ingestStreamWithRetries(managedSourceInfo, ingestionProperties);
        } catch (IOException e) {
            throw new IngestionClientException("Failed to read from stream.", e);
        }
    }

    private void closeStreamSafely(StreamSourceInfo streamSourceInfo) {
        try {
            streamSourceInfo.getStream().close();
        } catch (IOException e) {
            log.warn("Failed to close byte stream", e);
        }
    }

    /**
     * <p>setQueuingPolicyFactor</p>
     * A factor used to tune the policy that handles the logic over which data size would the client choose to directly
     * use queued ingestion instead of trying streaming ingestion first.
     * Setting the factor will create a new {@link ManagedStreamingQueuingPolicy} with this factor, which will be used
     * in the future ingestion calls.
     *
     * @param factor - Default is 1.
     **/
    public void setQueuingPolicyFactor(double factor) {
        this.queuingPolicy = new ManagedStreamingQueuingPolicy(factor);
    }

    @Override
    protected String getClientType() {
        return CLASS_NAME;
    }

    @Override
    public void close() throws IOException {
        queuedIngestClient.close();
        streamingIngestClient.close();
    }

    @Override
    public void setQueueRequestOptions(RequestRetryOptions queueRequestOptions) {
        queuedIngestClient.setQueueRequestOptions(queueRequestOptions);
    }

    @Override
    public IngestionResourceManager getResourceManager() {
        return queuedIngestClient.getResourceManager();
    }
}
