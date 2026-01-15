// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
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
interface IngestClient : Closeable {

    /**
     * Ingests data from the specified source.
     *
     * @param source The source to ingest.
     * @param ingestRequestProperties Ingestion properties containing database,
     *   table, format, and other settings.
     * @return An [IngestionOperation] object that can be used to track the
     *   status of the ingestion.
     */
    suspend fun ingestAsync(
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties,
    ): ExtendedIngestResponse

    /**
     * Get the current status of an ingestion operation.
     *
     * Unlike [getOperationDetailsAsync], this method returns only the summary
     * of the operation - statistics on the blobs ingested, and the operation
     * status.
     *
     * To use this method, the [IngestRequestProperties.enableTracking] property
     * must be set to true when ingesting the data.
     *
     * @param operation The ingestion operation to get the status for.
     * @return An [Status] object that provides a summary of the ingestion
     *   operation.
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
     * @return An [StatusResponse] object that provides detailed information
     *   about the ingestion operation.
     */
    suspend fun getOperationDetailsAsync(
        operation: IngestionOperation,
    ): StatusResponse
}

/** Interface for ingesting from multiple data sources into Kusto. */
interface MultiIngestClient : IngestClient {

    /**
     * Ingest data from multiple blob sources.
     *
     * **Important:** Multi-blob ingestion only supports [BlobSource]. This design avoids
     * partial failure scenarios where some local sources might be uploaded successfully
     * while others fail, leaving the user in an inconsistent state.
     *
     * **For local files/streams**, you have two options:
     *
     * 1. **Single-source ingestion**: Use `ingestAsync(source, properties)` with a single
     *    [com.microsoft.azure.kusto.ingest.v2.source.LocalSource] (FileSource or StreamSource).
     *    The client handles upload internally.
     *
     * 2. **Multi-source ingestion**: Use [com.microsoft.azure.kusto.ingest.v2.uploader.IUploader]
     *    to upload local sources to blob storage first, then call this method with the
     *    resulting [BlobSource] objects.
     *
     * @param sources The blob sources to ingest. All sources must be [BlobSource] instances.
     * @param ingestRequestProperties Ingestion properties containing database,
     *   table, format, and other settings.
     * @return An [ExtendedIngestResponse] containing the ingestion operation details.
     */
    suspend fun ingestAsync(
        sources: List<BlobSource>,
        ingestRequestProperties: IngestRequestProperties,
    ): ExtendedIngestResponse

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
    suspend fun getMaxSourcesPerMultiIngest(): Int
}
