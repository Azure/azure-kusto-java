package com.microsoft.azure.kusto.ingest;

import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.source.BlobSourceInfo;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import com.microsoft.azure.kusto.ingest.source.ResultSetSourceInfo;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;

public interface IngestClient {

    /**
     * Ingests a local file into the service
     *
     * @param fileSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return A result that could be used to query for status
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     */
    IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    /**
     * Ingests a blob (a file stored as an Azure Storage Blob) into the service
     *
     * @param blobSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return A result that could be used to query for status
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     */
    IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    /**
     * Ingests a ResultSet into the service
     *
     * @param resultSetSourceInfo The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return A result that could be used to query for status
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     */
    IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

    /**
     * Ingests a ResultSet into the service
     *
     * @param resultSetSourceInfo The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @param tempStoragePath     A local folder path that will be used as a temporary storage, data will be deleted on successful ingestion
     * @return A result that could be used to query for status
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     */
    IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties, String tempStoragePath) throws IngestionClientException, IngestionServiceException;

    /**
     * Ingests a InputStream into the service
     *
     * @param streamSourceInfo    The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return A result that could be used to query for status
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     */
    IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties) throws IngestionClientException, IngestionServiceException;

}
