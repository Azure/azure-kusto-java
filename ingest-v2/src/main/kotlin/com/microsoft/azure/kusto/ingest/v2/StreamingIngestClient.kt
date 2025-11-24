// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.FileSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.SourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.StreamSourceInfo
import io.ktor.http.HttpStatusCode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.net.ConnectException
import java.net.URI
import java.util.UUID

@Serializable
private data class StreamFromBlobRequestBody(
    @SerialName("SourceUri") val sourceUri: String,
)

class StreamingIngestClient(
    val engineUrl: String,
    override val tokenCredential: TokenCredential,
    override val skipSecurityChecks: Boolean = false,
    override val clientDetails: ClientDetails? = null,
) :
    KustoBaseApiClient(
        engineUrl,
        tokenCredential,
        skipSecurityChecks,
        clientDetails,
    ),
    IngestClient {

    /** Handles multiple source types for streaming ingestion. */
    override suspend fun submitIngestion(
        database: String,
        table: String,
        sources: List<SourceInfo>,
        format: Format,
        ingestProperties: IngestRequestProperties?,
    ): IngestResponse {
        require(sources.isNotEmpty()) { "At least one source is required" }

        // Streaming ingestion processes one source at a time
        val source = sources.first()
        val operationId = UUID.randomUUID().toString()

        when (source) {
            is BlobSourceInfo -> {
                logger.info(
                    "Streaming ingestion from BlobSource: ${source.blobPath}",
                )
                submitStreamingIngestion(
                    database = database,
                    table = table,
                    // Not used for blob-based streaming
                    data = ByteArray(0),
                    format = format,
                    ingestProperties = ingestProperties,
                    blobUrl = source.blobPath,
                )
            }
            is FileSourceInfo -> {
                logger.info(
                    "Streaming ingestion from FileSource: ${source.name}",
                )
                val data = source.data().readBytes()
                submitStreamingIngestion(
                    database = database,
                    table = table,
                    data = data,
                    format = format,
                    ingestProperties = ingestProperties,
                    blobUrl = null,
                )
                source.close()
            }
            is StreamSourceInfo -> {
                logger.info(
                    "Streaming ingestion from StreamSource: ${source.name}",
                )
                val data = source.data().readBytes()
                submitStreamingIngestion(
                    database = database,
                    table = table,
                    data = data,
                    format = format,
                    ingestProperties = ingestProperties,
                    blobUrl = null,
                )
                source.close()
            }
            else -> {
                throw IngestException(
                    message =
                    "Unsupported source type for streaming ingestion: ${source::class.simpleName}",
                    isPermanent = true,
                )
            }
        }

        // Streaming ingestion doesn't return an operation ID from the server
        // We generate one locally for consistency with the IngestClient interface
        return IngestResponse(ingestionOperationId = operationId)
    }

    /**
     * Note: Streaming ingestion doesn't support operation tracking. Throws
     * UnsupportedOperationException.
     */
    override suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean,
    ): StatusResponse {
        throw UnsupportedOperationException(
            "Streaming ingestion does not support operation status tracking. " +
                "Operation ID: $operationId cannot be tracked. ",
        )
    }

    /**
     * Note: Streaming ingestion doesn't support operation tracking. Throws
     * UnsupportedOperationException.
     */
    override suspend fun getIngestionDetails(
        database: String,
        table: String,
        operationId: String,
        details: Boolean,
    ): StatusResponse {
        throw UnsupportedOperationException(
            "Streaming ingestion does not support detailed operation tracking. " +
                "Operation ID: $operationId cannot be tracked. ",
        )
    }

    /**
     * Submits a streaming ingestion request.
     *
     * @param database The target database name
     * @param table The target table name
     * @param data The data to ingest (as ByteArray)
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @param blobUrl Optional blob URL for blob-based streaming ingestion (if
     *   provided, data is ignored)
     * @return IngestResponse for tracking the request
     */
    suspend fun submitStreamingIngestion(
        database: String,
        table: String,
        data: ByteArray,
        format: Format = Format.csv,
        ingestProperties: IngestRequestProperties? = null,
        blobUrl: String? = null,
    ) {
        val host = URI(engineUrl).host

        val bodyContent: Any
        val sourceKind: String?
        val contentType: String

        if (blobUrl != null) {
            // Blob-based streaming
            val requestBody = StreamFromBlobRequestBody(sourceUri = blobUrl)
            bodyContent = Json.encodeToString(requestBody).toByteArray()
            sourceKind = "uri"
            contentType = "application/json"
            logger.info(
                "Submitting streaming ingestion from blob for database: {}, table: {}, blob: {}. Host {}",
                database,
                table,
                blobUrl,
                host,
            )
        } else {
            // Direct streaming using raw data
            bodyContent = data
            sourceKind = null
            contentType = "application/octet-stream"
            logger.info(
                "Submitting streaming ingestion request for database: {}, table: {}, data size: {}. Host {}",
                database,
                table,
                data.size,
                host,
            )
        }

        try {
            val response: HttpResponse<Unit> =
                api.postStreamingIngest(
                    database = database,
                    table = table,
                    streamFormat = format,
                    body = bodyContent,
                    mappingName =
                    ingestProperties?.ingestionMappingReference,
                    sourceKind = sourceKind,
                    host = host,
                    acceptEncoding = "gzip",
                    connection = "Keep-Alive",
                    contentEncoding = null,
                    contentType = contentType,
                )
            return handleIngestResponse(
                response = response,
                database = database,
                table = table,
                dmUrl = engineUrl,
                endpointType = "streaming",
            )
        } catch (notAbleToReachHost: ConnectException) {
            val message =
                "Failed to reach $engineUrl for streaming ingestion. Please ensure the cluster address is correct and the cluster is reachable."
            throw IngestException(
                message = message,
                cause = notAbleToReachHost,
                failureCode = HttpStatusCode.NotFound.value,
                failureSubCode = "",
                isPermanent = false,
            )
        } catch (e: Exception) {
            logger.error(
                "Exception occurred during streaming ingestion submission",
                e,
            )
            if (e is IngestException) throw e
            throw IngestException(
                message =
                "Error submitting streaming ingest request to $engineUrl",
                cause = e,
                isPermanent = true,
            )
        }
    }
}
