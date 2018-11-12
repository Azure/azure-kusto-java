package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.*;

public interface IngestClient {

    IngestionResult ingestFromFile (FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromBlob (BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromResultSet (ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromStream (StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws Exception;

    IngestionResult ingestFromByteArray(ByteArraySourceInfo byteArraySourceInfo, IngestionProperties ingestionProperties) throws Exception;
}
