package com.microsoft.azure.kusto.data;

import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;

import java.io.InputStream;

public interface StreamingClient {

    /**
     * <p>Ingest data from a given stream directly into Kusto database.</p>
     * This method ingests the data from a given stream directly into Kusto database, using streaming ingestion endpoint,
     * with additional properties mentioned in {@code ingestionProperties}
     * It is not recommended to use this method directly, but the StreamingIngestClient from the Ingest package.
     *
     * @param database     The target database to ingest to
     * @param table        The target table to ingest to
     * @param stream       An InputStream which holds the data
     * @param properties   Additional request headers of the ingestion request
     * @param streamFormat The format of the data in the provided stream
     * @param mappingName  Pre-defined mapping reference. Required for Json and Avro formats
     * @param leaveOpen    Specifies if the given stream should be closed after reading or be left open
     * @return {@link KustoResponseResultSet} object including the ingestion result
     * @throws DataClientException  An exception originating from a client activity
     * @throws DataServiceException An exception returned from the service
     */
    KustoResponseResultSet executeStreamingIngest(String database, String table, InputStream stream, ClientRequestProperties properties, String streamFormat, String mappingName, boolean leaveOpen) throws DataServiceException, DataClientException;
}
