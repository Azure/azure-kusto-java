package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import java.io.InputStream;

public interface StreamingIngestProvider {

    /**
     * <p>Ingest data from a given stream directly into Kusto database.</p>
     * This method ingests the data from a given stream directly into Kusto database, using streaming ingestion API,
     * with additional properties mentioned in {@code ingestionProperties}
     *
     * @param database       The target database to ingest to
     * @param table          The target table to ingest to
     * @param stream         An InputStream which holds the data
     * @param properties     Additional request headers of the ingestion request
     * @param streamFormat   The format of the data in the provided stream
     * @param compressStream Specifies if the given stream is already compressed or should be compressed before ingesting
     * @param mappingName    Pre-defined mapping reference. Required for Json and Avro formats
     * @param leaveOpen      Specifies if the given stream should be closed after reading or be left open
     * @return {@link Results} object including the ingestion result
     * @throws DataClientException  An exception originating from a client activity
     * @throws DataServiceException An exception returned from the service
     */
    Results executeStreamingIngest(String database, String table, InputStream stream, ClientRequestProperties properties, String streamFormat, boolean compressStream, String mappingName, boolean leaveOpen) throws DataServiceException, DataClientException;
}
