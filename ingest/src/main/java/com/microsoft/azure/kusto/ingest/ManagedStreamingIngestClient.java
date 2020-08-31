package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.Semaphore;

//TODO: comment
public class ManagedStreamingIngestClient implements IngestClient {

    public static final int MAX_CONCURRENT_CALLS = 10;
    private final Semaphore concurrentStreamingIngestJobsSemaphore;
    private final IngestClientImpl queuedIngestClient;
    private final StreamingIngestClient streamingIngestClient;

    public ManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
                                        ConnectionStringBuilder engineConnectionStringBuilder) throws URISyntaxException {
        this(dmConnectionStringBuilder, engineConnectionStringBuilder, MAX_CONCURRENT_CALLS);
    }

    public ManagedStreamingIngestClient(ConnectionStringBuilder dmConnectionStringBuilder,
                                        ConnectionStringBuilder engineConnectionStringBuilder,
                                        int maxConcurrentCalls) throws URISyntaxException {
        concurrentStreamingIngestJobsSemaphore = new Semaphore(MAX_CONCURRENT_CALLS);
        queuedIngestClient = new IngestClientImpl(dmConnectionStringBuilder);
        streamingIngestClient = new StreamingIngestClient(engineConnectionStringBuilder);
    }


    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        return null;
    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        return null;
    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        return null;
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
