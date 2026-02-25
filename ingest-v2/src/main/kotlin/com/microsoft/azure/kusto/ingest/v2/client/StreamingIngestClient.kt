// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.STREAMING_MAX_REQ_BODY_SIZE
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestRequestException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestServiceException
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.models.withFormatFromSource
import com.microsoft.azure.kusto.ingest.v2.common.utils.IngestionUtils
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.FileSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.future
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.URI
import java.util.UUID
import java.util.concurrent.CompletableFuture

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

    /**
     * Ingests data from the specified source with the given properties. This is
     * the suspend function for Kotlin callers.
     *
     * @param source The source to ingest (FileSource, StreamSource, or
     *   BlobSource).
     * @param ingestRequestProperties Ingestion properties containing database,
     *   table, format, and other settings.
     * @return An ExtendedIngestResponse containing the operation ID and
     *   ingestion kind.
     */
    override suspend fun ingestAsync(
        database: String,
        table: String,
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse =
        ingestAsyncInternal(
            database,
            table,
            source,
            ingestRequestProperties,
        )

    /**
     * Ingests data from the specified source with the given properties. This is
     * the Java-friendly version that returns a CompletableFuture.
     *
     * @param source The source to ingest (FileSource, StreamSource, or
     *   BlobSource).
     * @param ingestRequestProperties Ingestion properties containing database,
     *   table, format, and other settings.
     * @return A CompletableFuture that completes with an
     *   ExtendedIngestResponse.
     */
    override fun ingestAsyncJava(
        database: String,
        table: String,
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties?,
    ): CompletableFuture<ExtendedIngestResponse> =
        CoroutineScope(Dispatchers.IO).future {
            ingestAsyncInternal(
                database,
                table,
                source,
                ingestRequestProperties,
            )
        }

    /**
     * Gets the operation summary for the specified ingestion operation. This is
     * the Java-friendly version that returns a CompletableFuture.
     *
     * Note: Streaming ingestion does not support operation tracking, so this
     * always returns an empty status.
     *
     * @param operation The ingestion operation to get the status for.
     * @return A CompletableFuture that completes with a Status object.
     */
    override fun getOperationSummaryAsyncJava(
        operation: IngestionOperation,
    ): CompletableFuture<Status> =
        CoroutineScope(Dispatchers.IO).future {
            getOperationSummaryAsync(operation)
        }

    /**
     * Gets the detailed operation status for the specified ingestion operation.
     * This is the Java-friendly version that returns a CompletableFuture.
     *
     * Note: Streaming ingestion does not support operation tracking, so this
     * always returns an empty status response.
     *
     * @param operation The ingestion operation to get the details for.
     * @return A CompletableFuture that completes with a StatusResponse object.
     */
    override fun getOperationDetailsAsyncJava(
        operation: IngestionOperation,
    ): CompletableFuture<StatusResponse> =
        CoroutineScope(Dispatchers.IO).future {
            getOperationDetailsAsync(operation)
        }

    /**
     * Internal implementation of ingestAsync that both the suspend and Java
     * versions call.
     */
    private suspend fun ingestAsyncInternal(
        database: String,
        table: String,
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse {
        // Inject format from source into properties
        val effectiveProperties =
            ingestRequestProperties?.withFormatFromSource(source)
                ?: IngestRequestPropertiesBuilder.create()
                    .build()
                    .withFormatFromSource(source)
        // Streaming ingestion processes one source at a time
        val maxSize =
            getMaxStreamingIngestSize(
                compressionType = source.compressionType,
                format = effectiveProperties.format,
            )
        val operationId = UUID.randomUUID().toString()
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
                    ingestProperties = effectiveProperties,
                    blobUrl = source.blobPath,
                    compressionType = source.compressionType,
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
                    ingestProperties = effectiveProperties,
                    blobUrl = null,
                    compressionType = source.compressionType,
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
        ingestProperties: IngestRequestProperties?,
        blobUrl: String? = null,
        compressionType: CompressionType,
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
            val response =
                this.apiClient.api.postStreamingIngest(
                    database = database,
                    table = table,
                    streamFormat =
                    ingestProperties?.format ?: Format.csv,
                    body = bodyContent,
                    mappingName =
                    ingestProperties?.ingestionMappingReference,
                    sourceKind = sourceKind,
                    host = host,
                    acceptEncoding = null,
                    connection = "Keep-Alive",
                    contentEncoding =
                    if (
                        compressionType ==
                        CompressionType.GZIP
                    ) {
                        "gzip"
                    } else {
                        null
                    },
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
            "Streaming ingestion does not support operation status tracking. Operation ID: ${operation.operationId} " +
                "cannot be tracked. Returning empty status.",
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

            // Try to parse the error response as Kusto OneApiError format
            val errorDetails = parseKustoErrorResponse(response)

            val errorMessage =
                if (errorDetails != null) {
                    // Use the detailed message from the Kusto error response
                    val description =
                        errorDetails.description ?: errorDetails.message
                    "Failed to submit streaming ingestion to $database.$table. " +
                        "Error: $description (Code: ${errorDetails.code}, Type: ${errorDetails.type})"
                } else {
                    // Fallback to generic error message
                    "Failed to submit streaming ingestion to $database.$table. Status: ${response.status}"
                }

            logger.error(errorMessage)

            // Determine if the error is permanent based on the parsed response
            val isPermanent = errorDetails?.permanent ?: true
            val failureCode = errorDetails?.failureCode ?: response.status

            // Use appropriate exception type based on the error
            if (isPermanent) {
                throw IngestRequestException(
                    errorCode = errorDetails?.code,
                    errorReason = errorDetails?.type,
                    errorMessage =
                    errorDetails?.description
                        ?: errorDetails?.message,
                    databaseName = database,
                    failureCode = failureCode,
                    isPermanent = true,
                    message = errorMessage,
                )
            } else {
                throw IngestServiceException(
                    errorCode = errorDetails.code,
                    errorReason = errorDetails.type,
                    errorMessage =
                    errorDetails.description
                        ?: errorDetails.message,
                    failureCode = failureCode,
                    isPermanent = false,
                    message = errorMessage,
                )
            }
        }
    }

    /**
     * Parses the Kusto error response to extract error details. The error
     * response follows the OneApiError format:
     * ```json
     * {
     *   "error": {
     *     "code": "BadRequest",
     *     "message": "Request is invalid and cannot be executed.",
     *     "@type": "Kusto.DataNode.Exceptions.StreamingIngestionRequestException",
     *     "@message": "Bad streaming ingestion request...",
     *     "@failureCode": 400,
     *     "@permanent": true
     *   }
     * }
     * ```
     */
    private suspend fun <T : Any> parseKustoErrorResponse(
        response: HttpResponse<T>,
    ): KustoErrorDetails? {
        return try {
            val bodyText = response.response.bodyAsText()
            if (bodyText.isBlank()) {
                logger.debug("Empty error response body")
                return null
            }

            logger.debug("Parsing error response: {}", bodyText)

            val json = Json { ignoreUnknownKeys = true }
            val rootObject = json.parseToJsonElement(bodyText).jsonObject

            // The error is wrapped in an "error" object
            val errorObject = rootObject["error"]?.jsonObject
            if (errorObject == null) {
                logger.debug("No 'error' field found in response")
                return null
            }

            val code = errorObject["code"]?.jsonPrimitive?.content
            val message = errorObject["message"]?.jsonPrimitive?.content
            val type = errorObject["@type"]?.jsonPrimitive?.content
            val description = errorObject["@message"]?.jsonPrimitive?.content
            val failureCode = errorObject["@failureCode"]?.jsonPrimitive?.int
            val permanent =
                errorObject["@permanent"]?.jsonPrimitive?.boolean ?: true

            KustoErrorDetails(
                code = code,
                message = message,
                type = type,
                description = description,
                failureCode = failureCode,
                permanent = permanent,
            )
        } catch (e: Exception) {
            logger.warn("Failed to parse Kusto error response: ${e.message}", e)
            null
        }
    }

    private fun getMaxStreamingIngestSize(
        compressionType: CompressionType,
        format: Format,
    ): Long {
        val compressionFactor =
            IngestionUtils.getRowStoreEstimatedFactor(
                format,
                compressionType,
            )
        return (STREAMING_MAX_REQ_BODY_SIZE * compressionFactor).toLong()
    }

    override fun close() {}
}

@Serializable
private data class StreamFromBlobRequestBody(
    @SerialName("SourceUri") val sourceUri: String,
)

/**
 * Data class to hold parsed Kusto error details from OneApiError format.
 * Matches the structure of error responses from Kusto streaming ingestion.
 */
private data class KustoErrorDetails(
    /** The error code (e.g., "BadRequest") */
    val code: String?,
    /** The high-level error message */
    val message: String?,
    /** The exception type (from @type field) */
    val type: String?,
    /** The detailed error description (from @message field) */
    val description: String?,
    /** The HTTP failure code (from @failureCode field) */
    val failureCode: Int?,
    /** Whether the error is permanent (from @permanent field) */
    val permanent: Boolean,
)
