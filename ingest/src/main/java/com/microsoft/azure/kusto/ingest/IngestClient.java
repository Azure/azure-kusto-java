package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

import java.util.concurrent.CompletableFuture;

public interface IngestClient {

    IngestionResult ingestFromFile (FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    IngestionResult ingestFromBlob (BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    IngestionResult ingestFromResultSet (ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    IngestionResult ingestFromStream (StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    CompletableFuture<IngestionResult> ingestFromFileAsync (FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties);

    CompletableFuture<IngestionResult> ingestFromStreamAsync (StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties);

    CompletableFuture<IngestionResult> ingestFromBlobAsync (BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties);

}
