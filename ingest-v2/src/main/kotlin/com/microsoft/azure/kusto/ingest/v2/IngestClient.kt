// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.SourceInfo
import io.ktor.http.HttpStatusCode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.ConnectException

/**
 * interface with provides core abstraction for ingesting data into Kusto.
 *
 * Supports multiple source types:
 * - BlobSourceInfo: Ingest from Azure Blob Storage
 * - FileSourceInfo: Ingest from local files
 * - StreamSourceInfo: Ingest from in-memory streams
 */
interface IngestClient {

    val logger: Logger
        get() = LoggerFactory.getLogger(IngestClient::class.java)

    /**
     * Submits an ingestion request from any source type.
     *
     * @param database The target database name
     * @param table The target table name
     * @param sources List of sources to ingest (BlobSourceInfo, FileSourceInfo,
     *   or StreamSourceInfo)
     * @param format The data format (CSV, JSON, others)
     * @param ingestProperties Optional ingestion properties
     * @return IngestResponse containing the operation ID for tracking
     */
    suspend fun submitIngestion(
        database: String,
        table: String,
        sources: List<SourceInfo>,
        format: Format = Format.csv,
        ingestProperties: IngestRequestProperties? = null,
    ): IngestResponse

    /**
     * Gets the status of an ingestion operation.
     *
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from submitIngestion
     * @param forceDetails Whether to force retrieval of detailed information
     * @return StatusResponse containing the current status
     */
    suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean = false,
    ): StatusResponse

    /**
     * Gets detailed information about an ingestion operation.
     *
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from submitIngestion
     * @param details Whether to retrieve detailed blob-level information
     * @return StatusResponse containing operation details
     * @throws UnsupportedOperationException if the implementation doesn't
     *   support operation tracking
     */
    suspend fun getIngestionDetails(
        database: String,
        table: String,
        operationId: String,
        details: Boolean = true,
    ): StatusResponse

    // Common way to parse ingestion response for both Streaming and Queued ingestion

    suspend fun <T : Any> handleIngestResponse(
        response: HttpResponse<T>,
        database: String,
        table: String,
        dmUrl: String,
        endpointType: String,
    ): T {
        if (response.success) {
            val ingestResponseBody = response.body()
            return ingestResponseBody
        } else {
            if (response.status == HttpStatusCode.NotFound.value) {
                val message =
                    "Endpoint $dmUrl not found. Please ensure the cluster supports $endpointType ingestion."
                logger.error(
                    "$endpointType ingestion endpoint not found. Please ensure that the target cluster supports $endpointType ingestion and that the endpoint URL is correct.",
                )
                throw IngestException(
                    message = message,
                    cause = ConnectException(message),
                    failureCode = response.status,
                    failureSubCode = "",
                    isPermanent = false,
                )
            }
            val nonSuccessResponseBody: T = response.body()
            val ingestResponseOperationId =
                if (nonSuccessResponseBody is IngestResponse) {
                    if (
                        (nonSuccessResponseBody as IngestResponse)
                            .ingestionOperationId != null
                    ) {
                        logger.info(
                            "Ingestion Operation ID: ${(nonSuccessResponseBody as IngestResponse).ingestionOperationId}",
                        )
                        nonSuccessResponseBody.ingestionOperationId
                    } else {
                        "N/A"
                    }
                } else {
                    "N/A"
                }
            val errorMessage =
                "Failed to submit $endpointType ingestion to $database.$table. " +
                    "Status: ${response.status}, Body: $nonSuccessResponseBody. " +
                    "OperationId $ingestResponseOperationId"
            logger.error(
                "$endpointType ingestion failed with response: {}",
                errorMessage,
            )
            throw IngestException(
                message = errorMessage,
                cause = RuntimeException(errorMessage),
                isPermanent = true,
            )
        }
    }
}
