package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.concurrent.Semaphore;

//TODO: comment
public class ManagedStreamingIngestClient implements IngestClient {

    private final static Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
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
        log.info("Creating a new ManagedStreamingIngestClient");
        concurrentStreamingIngestJobsSemaphore = new Semaphore(maxConcurrentCalls);
        queuedIngestClient = new IngestClientImpl(dmConnectionStringBuilder);
        streamingIngestClient = new StreamingIngestClient(engineConnectionStringBuilder);
    }


    @Override
    public IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(fileSourceInfo, "fileSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        fileSourceInfo.validate();
        ingestionProperties.validate();    }

    @Override
    public IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(blobSourceInfo, "blobSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        blobSourceInfo.validate();
        ingestionProperties.validate();    }

    @Override
    public IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(resultSetSourceInfo, "resultSetSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        resultSetSourceInfo.validate();
        ingestionProperties.validate();
    }

    @Override
    public IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException {
        Ensure.argIsNotNull(streamSourceInfo, "streamSourceInfo");
        Ensure.argIsNotNull(ingestionProperties, "ingestionProperties");

        streamSourceInfo.validate();
        ingestionProperties.validate();

        if (streamSourceInfo.isLeaveOpen())
        {
            throw new UnsupportedOperationException("Stream can't be Leave Open in ManagedStreamingIngestClient");
        }

        //todo impl
    }

    @Override
    public void close() throws IOException {
        queuedIngestClient.close();
        streamingIngestClient.close();
    }
}
