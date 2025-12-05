package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
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
import com.microsoft.azure.kusto.ingest.v2.uploaders.IUploader
import io.ktor.http.HttpStatusCode
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.net.URI
import java.util.UUID

class StreamingIngestClient : IIngestClient {
    private val logger = LoggerFactory.getLogger(StreamingIngestClient::class.java)
    val apiClient: KustoBaseApiClient
    val cachedConfiguration: ConfigurationCache
    val uploader: IUploader
    val shouldDisposeUploader: Boolean

    internal constructor(
        apiClient: KustoBaseApiClient,
        cachedConfiguration: ConfigurationCache,
        uploader: IUploader,
        shouldDisposeUploader: Boolean = false,
    ) {
        this.apiClient = apiClient
        this.cachedConfiguration = cachedConfiguration
        this.uploader = uploader
        this.shouldDisposeUploader = shouldDisposeUploader
    }

    override suspend fun ingestAsync(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties?
    ): IngestResponse {
        // Streaming ingestion processes one source at a time
        val operationId = UUID.randomUUID().toString()
        val effectiveIngestionProperties =
            props ?: IngestRequestProperties(format = Format.csv)
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
            is FileSource -> {
                logger.info(
                    "Streaming ingestion from FileSource: ${source.name}",
                )
                val data = source.data().readBytes()
                submitStreamingIngestion(
                    database = database,
                    table = table,
                    data = data,
                    ingestProperties = effectiveIngestionProperties,
                    blobUrl = null,
                )
                source.close()
            }
            is StreamSource -> {
                logger.info(
                    "Streaming ingestion from StreamSource: ${source.name}",
                )
                val data = source.data().readBytes()
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
        return IngestResponse(ingestionOperationId = operationId)
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
                isPermanent = true,
            )
        }
    }

    override suspend fun getOperationSummaryAsync(operation: IngestionOperation): Status {
        throw UnsupportedOperationException(
            "Streaming ingestion does not support detailed operation tracking. " +
                    "Operation ID: ${operation.operationId} cannot be tracked. ",
        )
    }

    override suspend fun getOperationDetailsAsync(operation: IngestionOperation): StatusResponse {
        throw UnsupportedOperationException(
            "Streaming ingestion does not support operation status tracking. " +
                    "Operation ID: ${operation.operationId} cannot be tracked. ",
        )
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
                    failureSubCode = UploadErrorCode.NETWORK_ERROR.toString(),
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

    override fun close() {
        TODO("Not yet implemented")
    }
}

@Serializable
private data class StreamFromBlobRequestBody(
    @SerialName("SourceUri") val sourceUri: String,
)