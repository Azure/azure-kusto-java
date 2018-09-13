package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

public interface IngestClient {

    IngestionResult ingestFromFile (FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromBlob (BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromResultSet (ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromStream (StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws Exception;




}
