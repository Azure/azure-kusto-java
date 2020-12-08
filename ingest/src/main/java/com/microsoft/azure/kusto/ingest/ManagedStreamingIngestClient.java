package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.StreamingClient;
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

import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;

/**
 * <p>ManagedStreamingIngestClient</p>
 * <p>
 * This class combines a managed streaming client with a queued streaming client, to create an optimized experience.
 * Since the streaming client communicates directly with the engine, it's more prone to failure, so this class
 * holds both a streaming client and a queued client.
 * It tries {@value MAX_RETRY_CALLS} times using the streaming client, after which it falls back to the queued streaming client in case of failure.
 * <p>
 * Note that {@code ingestFromBlob} behaves differently from the other methods - since a blob already exists it makes more sense to enqueue it rather than downloading and streaming it, thus ManagedStreamingIngestClient skips the streaming retries and sends it directly to the queued client.
 */
public class ManagedStreamingIngestClient implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final int MAX_RETRY_CALLS = 3;
    private final QueuedIngestClient queuedIngestClient;
    private final StreamingIngestClient streamingIngestClient;

    public ManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
                                        ConnectionStringBuilder engineConnectionStringBuilder) throws URISyntaxException {
        log.info("Creating a new ManagedStreamingIngestClient from connection strings");
        queuedIngestClient = new QueuedIngestClient(dmConnectionStringBuilder);
        streamingIngestClient = new StreamingIngestClient(engineConnectionStringBuilder);
    }

    public ManagedStreamingIngestClient(ResourceManager resourceManager,
                                        AzureStorageClient storageClient,
                                        StreamingClient streamingClient) {
        log.info("Creating a new ManagedStreamingIngestClient from raw parts");
        queuedIngestClient = new QueuedIngestClient(resourceManager, storageClient);
        streamingIngestClient = new StreamingIngestClient(streamingClient);
    }

    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
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
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();

        // If it's a blob we ingest using the queued client
        return queuedIngestClient.ingestFromBlob(blobSourceInfo, ingestionProperties);
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validate();
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
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        if (streamSourceInfo.isLeaveOpen()) {
            throw new UnsupportedOperationException("LeaveOpen can't be true in ManagedStreamingIngestClient");
        }
        streamSourceInfo.setLeaveOpen(true);

        try {
            for (int i = 0; i < MAX_RETRY_CALLS; i++) {
                try {
                    return streamingIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
                }
                catch (Exception e) {
                    if (e instanceof IngestionServiceException
                            && e.getCause() != null
                            && e.getCause() instanceof  DataServiceException
                            && e.getCause().getCause() != null
                            && e.getCause().getCause() instanceof DataWebException) {
                        DataWebException webException = (DataWebException) e.getCause().getCause();
                        try {
                            OneApiError oneApiError = webException.getApiError();
                            if (oneApiError.isPermanent())
                            {
                                log.error("Error is permanent, stopping.");
                                throw e;
                            }
                        } catch (JSONException je)
                        {
                            log.info("Failed to parse json in exception, continuing.", je);
                        }
                    }

                    log.info("Streaming ingestion failed, trying again", e);
                    try {
                        streamSourceInfo.getStream().reset();
                    } catch (IOException ioException) {
                        throw new IngestionClientException("Ingestion failed transiently but the stream isn't resettable therefore ingestion wasn't retried", ioException);
                    }
                }
            }

            return queuedIngestClient.ingestFromStream(streamSourceInfo, ingestionProperties);
        } finally {
            try {
                streamSourceInfo.getStream().close();
            } catch (IOException e) {
                log.warn("Failed to close stream", e);
            }
        }
    }

    @Override
    public void close() {
        queuedIngestClient.close();
        streamingIngestClient.close();
    }
}
