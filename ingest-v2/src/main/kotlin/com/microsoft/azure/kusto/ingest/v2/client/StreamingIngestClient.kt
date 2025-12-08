// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.STREAMING_MAX_REQ_BODY_SIZE
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.utils.IngestionUtils
import com.microsoft.azure.kusto.ingest.v2.container.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.FileSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.URI
import java.util.UUID

/**
 * Streaming ingestion client for Azure Data Explorer (Kusto).
 *
 * This client handles ingestion through the streaming ingestion path, which
 * provides direct, synchronous data ingestion suitable for low-latency
 * scenarios.
 *
 * **Important:** This class cannot be instantiated directly. Use
 * [com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder]
 * to create instances of this client. The internal constructor ensures that
 * only the builder can create properly configured instances.
 *
 * **Note:** Streaming ingestion does not support operation tracking. The
 * methods [getOperationSummaryAsync] and [getOperationDetailsAsync] will return
 * empty responses with warnings logged.
 *
 * Example usage:
 * ```
 * val client = StreamingIngestClientBuilder.create(engineEndpoint)
 *     .withAuthentication(tokenProvider)
 *     .build()
 * ```
 *
 * @see
 *   com.microsoft.azure.kusto.ingest.v2.builders.StreamingIngestClientBuilder
 */
class StreamingIngestClient
internal constructor(private val apiClient: KustoBaseApiClient) : IngestClient {
    private val logger =
        LoggerFactory.getLogger(StreamingIngestClient::class.java)

    companion object {
        private val EMPTY_STATUS =
                Status(
                    succeeded = 0L,
                    failed = 0L,
                    inProgress = 0L,
                    canceled = 0L,
                )

        private val EMPTY_STATUS_RESPONSE =
                StatusResponse(
                    status = EMPTY_STATUS,
                    details = emptyList(),
                    startTime = null,
                )
    }

    override suspend fun ingestAsync(
        source: IngestionSource,
        database: String,
        table: String,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse {
        // Streaming ingestion processes one source at a time
        val maxSize = getMaxStreamingIngestSize(source = source)
        val operationId = UUID.randomUUID().toString()
        val effectiveIngestionProperties =
            ingestRequestProperties
                ?: IngestRequestPropertiesBuilder(format = Format.csv)
                    .build()
        when (source) {
            is BlobSource -> {
                logger.info(
                    "Streaming ingestion from BlobSource: ${source.blobPath}",
                )
                submitStreamingIngestion(
                    database = database,
                    table = table,
                    // Not used for blob-based streaming
                    data = ByteArray(0),
                    ingestProperties = effectiveIngestionProperties,
                    blobUrl = source.blobPath,
                )
            }
            is FileSource,
            is StreamSource,
            -> {
                val name =
                    when (source) {
                        is FileSource -> source.name
                        is StreamSource -> source.name
                        else -> "UnknownSource"
                    }
                logger.info(
                    "Streaming ingestion from ${source::class.simpleName}: $name",
                )
                val data = source.data().readBytes()
                val contentSize = data.size
                if (contentSize > maxSize) {
                    val message =
                        "Request content size $contentSize exceeds the maximum allowed size of $STREAMING_MAX_REQ_BODY_SIZE bytes."
                    throw IngestException(message = message, isPermanent = true)
                }
                submitStreamingIngestion(
                    database = database,
                    table = table,
                    data = data,
                    ingestProperties = effectiveIngestionProperties,
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
        return ExtendedIngestResponse(
            IngestResponse(ingestionOperationId = operationId),
            IngestKind.STREAMING,
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
        ingestProperties: IngestRequestProperties,
        blobUrl: String? = null,
    ) {
        val host = URI(this.apiClient.engineUrl).host

        val bodyContent: Any
        val sourceKind: String?
        val contentType: String

        if (blobUrl != null) {
            // Blob-based streaming
            val requestBody = StreamFromBlobRequestBody(sourceUri = blobUrl)
            bodyContent = Json.encodeToString(requestBody).toByteArray()
            sourceKind = "uri"
            contentType = ContentType.Application.Json.toString()
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
            contentType = ContentType.Application.OctetStream.toString()
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
                this.apiClient.api.postStreamingIngest(
                    database = database,
                    table = table,
                    streamFormat = ingestProperties.format,
                    body = bodyContent,
                    mappingName =
                    ingestProperties.ingestionMappingReference,
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
                engineUrl = host,
            )
        } catch (notAbleToReachHost: ConnectException) {
            val message =
                "Failed to reach ${this.apiClient.engineUrl} for streaming ingestion. Please ensure the cluster address is correct and the cluster is reachable."
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
                "Error submitting streaming ingest request to ${this.apiClient.engineUrl}",
                cause = e,
                failureSubCode =
                UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
                    .toString(),
                isPermanent = true,
            )
        }
    }

    override suspend fun getOperationSummaryAsync(
        operation: IngestionOperation,
    ): Status {
        logger.warn(
            "Streaming ingestion does not support operation status tracking. Operation ID: ${operation.operationId} cannot be tracked. Returning empty status.",
        )
        return EMPTY_STATUS
    }

    override suspend fun getOperationDetailsAsync(
        operation: IngestionOperation,
    ): StatusResponse {
        logger.warn(
            "Streaming ingestion does not support detailed operation tracking. Operation ID: ${operation.operationId} cannot be tracked. Returning empty status response.",
        )
        return EMPTY_STATUS_RESPONSE
    }

    suspend fun <T : Any> handleIngestResponse(
        response: HttpResponse<T>,
        database: String,
        table: String,
        engineUrl: String,
    ): T {
        if (response.success) {
            val ingestResponseBody = response.body()
            return ingestResponseBody
        } else {
            if (response.status == HttpStatusCode.NotFound.value) {
                val message =
                    "Endpoint $engineUrl not found. Please ensure the cluster is reachable and supports streaming ingestion."
                logger.error(
                    "$engineUrl streaming endpoint not found. Please ensure that the target cluster supports " +
                        "streaming ingestion and that the endpoint URL is correct.",
                )
                throw IngestException(
                    message = message,
                    cause = ConnectException(message),
                    failureCode = response.status,
                    failureSubCode =
                    UploadErrorCode.NETWORK_ERROR.toString(),
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
                "Failed to submit streaming ingestion to $database.$table. " +
                    "Status: ${response.status}, Body: $nonSuccessResponseBody. " +
                    "OperationId $ingestResponseOperationId"
            logger.error(errorMessage)
            throw IngestException(
                message = errorMessage,
                cause = RuntimeException(errorMessage),
                isPermanent = true,
            )
        }
    }

    private fun getMaxStreamingIngestSize(source: IngestionSource): Long {
        val compressionFactor =
            IngestionUtils.getRowStoreEstimatedFactor(
                source.format,
                source.compressionType,
            )
        return (STREAMING_MAX_REQ_BODY_SIZE * compressionFactor).toLong()
    }

    override fun close() {}
}

@Serializable
private data class StreamFromBlobRequestBody(
    @SerialName("SourceUri") val sourceUri: String,
)
