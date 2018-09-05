package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.KustoIngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

public interface KustoIngestClient {

    KustoIngestionResult ingestFromFile (FileSourceInfo fileSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception;

    KustoIngestionResult ingestFromBlob (BlobSourceInfo blobSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception;

    KustoIngestionResult ingestFromResultSet (ResultSetSourceInfo resultSetSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception;

    KustoIngestionResult ingestFromStream (StreamSourceInfo streamSourceInfo, KustoIngestionProperties ingestionProperties) throws Exception;




}
