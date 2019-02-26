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
     * <p>Ingest data from a file into Kusto database.</p>
     * This method ingests the data from a given file, described in {@code fileSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param fileSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see FileSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromFile(FileSourceInfo fileSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    /**
     * <p>Ingest data from a blob storage into Kusto database.</p>
     * This method ingests the data from a given blob, described in {@code blobSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param blobSourceInfo      The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see BlobSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromBlob(BlobSourceInfo blobSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    /**
     * <p>Ingest data from a Result Set into Kusto database.</p>
     * This method ingests the data from a given Result Set, described in {@code resultSetSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param resultSetSourceInfo The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see ResultSetSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

    /**
     * <p>Ingest data from a Result Set into Kusto database, using a local temp storage.</p>
     * This method ingests the data from a given Result Set, described in {@code resultSetSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param resultSetSourceInfo The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @param tempStoragePath     A local folder path that will be used as a temporary storage, data will be deleted on successful ingestion
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see ResultSetSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromResultSet(ResultSetSourceInfo resultSetSourceInfo, IngestionProperties ingestionProperties, String tempStoragePath)
            throws IngestionClientException, IngestionServiceException;

    /**
     * <p>Ingest data from an input stream, into Kusto database.</p>
     * This method ingests the data from a given input stream, described in {@code streamSourceInfo}, into Kusto database,
     * according to the properties mentioned in {@code ingestionProperties}
     *
     * @param streamSourceInfo    The specific SourceInfo to be ingested
     * @param ingestionProperties Settings used to customize the ingestion operation
     * @return {@link IngestionResult} object including the ingestion result
     * @throws IngestionClientException  An exception originating from a client activity
     * @throws IngestionServiceException An exception returned from the service
     * @see StreamSourceInfo
     * @see IngestionProperties
     */
    IngestionResult ingestFromStream(StreamSourceInfo streamSourceInfo, IngestionProperties ingestionProperties)
            throws IngestionClientException, IngestionServiceException;

}
