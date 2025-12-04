// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import java.io.Closeable

/**
 * Interface for ingesting data into Kusto.
 *
 * Ingestion can be done from:
 * - A local file (see [com.microsoft.azure.kusto.ingest.v2.source.FileSource])
 * - A stream (see [com.microsoft.azure.kusto.ingest.v2.source.StreamSource])
 * - A blob (see [BlobSource])
 *
 * To track the result, set the [IngestRequestProperties.enableTracking]
 * property to true. Then you can use the [getOperationSummaryAsync] and
 * [getOperationDetailsAsync] methods to get the status of the ingestion
 * operation.
 */
interface IIngestClient : Closeable {

    /**
     * Ingests data from the specified source into the specified database and
     * table.
     *
     * @param source The source to ingest.
     * @param database The name of the database to ingest to.
     * @param table The name of the table to ingest to.
     * @param props Optional ingestion properties.
     * @return An [IngestionOperation] object that can be used to track the
     *   status of the ingestion.
     */
    suspend fun ingestAsync(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties? = null,
    ): IngestResponse

    /**
     * Get the current status of an ingestion operation.
     *
     * Unlike [getOperationDetailsAsync], this method returns only the summary
     * of the operation - statistics on the blobs ingested, and the operation
     * status.
     *
     * To use this method, the [IngestProperties.enableTracking] property must
     * be set to true when ingesting the data.
     *
     * @param operation The ingestion operation to get the status for.
     * @return An [OperationSummary] object that provides a summary of the
     *   ingestion operation.
     */
    suspend fun getOperationSummaryAsync(operation: IngestionOperation): Status

    /**
     * Get the current status of an ingestion operation.
     *
     * This method returns detailed information about the operation - statistics
     * on the blobs ingested, and the operation status, as well as specific
     * results for each blob.
     *
     * To use this method, the [IngestRequestProperties.enableTracking] property
     * must be set to true when ingesting the data.
     *
     * @param operation The ingestion operation to get the status for.
     * @return An [OperationDetails] object that provides detailed information
     *   about the ingestion operation.
     */
    suspend fun getOperationDetailsAsync(
        operation: IngestionOperation,
    ): StatusResponse
}

/** Interface for ingesting from multiple data sources into Kusto. */
interface IMultiIngestClient : IIngestClient {

    /**
     * Ingest data from multiple sources.
     *
     * @param sources The sources to ingest.
     * @param database The name of the database to ingest to.
     * @param table The name of the table to ingest to.
     * @param props Optional ingestion properties.
     * @return An [IngestionOperation] object that can be used to track the
     *   status of the ingestion.
     */
    suspend fun ingestAsync(
        sources: List<BlobSource>,
        database: String,
        table: String,
        props: IngestRequestProperties? = null,
    ): IngestResponse

    /**
     * Returns the maximum number of sources that can be ingested in a single
     * call to [ingestAsync].
     *
     * This limit is imposed to avoid excessively large requests that could lead
     * to performance degradation or failures.
     *
     * @return The maximum number of sources allowed in a single ingestion
     *   request.
     */
    fun getMaxSourcesPerMultiIngest(): Int
}
