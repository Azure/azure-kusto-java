// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestClientException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestSizeLimitExceededException
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Blob
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequest
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploaders.IUploader
import io.ktor.http.HttpStatusCode
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.time.Clock
import java.time.OffsetDateTime

class QueuedIngestClient : IMultiIngestClient {
    private val logger = LoggerFactory.getLogger(QueuedIngestClient::class.java)

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
        sources: List<BlobSource>,
        database: String,
        table: String,
        props: IngestRequestProperties?,
    ): IngestResponse {
        // Validate sources list is not empty
        require(sources.isNotEmpty()) { "sources list cannot be empty" }
        val configuration = cachedConfiguration.getConfiguration()
        val maxBlobsPerBatch =
            configuration.ingestionSettings?.maxBlobsPerBatch ?: 1
        val maxBlobsPerBatchInt = maxBlobsPerBatch.toInt()
        // Check if sources count exceeds the limit
        if (sources.size > maxBlobsPerBatchInt) {
            throw IngestSizeLimitExceededException(
                size = sources.size.toLong(),
                maxSize = maxBlobsPerBatchInt.toLong(),
                message =
                "Ingestion sources count(${sources.size}) is larger than the limit allowed ($maxBlobsPerBatchInt)",
                isPermanent = true,
            )
        }

        // Check that all blobs have the same format
        val firstFormat = sources.first().format
        val differentFormatBlob = sources.find { it.format != firstFormat }
        if (differentFormatBlob != null) {
            logger.error("All blobs in the request must have the same format")
            throw IngestClientException(
                "All blobs in the request must have the same format. " +
                    "Expected: $firstFormat, but found: ${differentFormatBlob.format}",
            )
        }

        // Check for duplicate blob URLs
        val duplicates =
            sources.groupBy { sanitizeBlobUrl(it.blobPath) }
                .filter { it.value.size > 1 }

        if (duplicates.isNotEmpty()) {
            val duplicateInfo =
                duplicates.entries.joinToString(", ") { (url, blobs) ->
                    val sourceIds =
                        blobs.joinToString(", ") {
                            it.sourceId.toString()
                        }
                    "{Url: $url, Source Ids: [$sourceIds]}"
                }
            throw IngestClientException(
                "Duplicate blob sources detected in the request: [$duplicateInfo]",
            )
        }
        val blobs =
            sources.map {
                Blob(
                    it.blobPath,
                    sourceId = it.sourceId.toString(),
                    rawSize = it.blobExactSize ?: 0L,
                )
            }
        val ingestRequest =
            IngestRequest(
                timestamp = OffsetDateTime.now(Clock.systemUTC()),
                blobs = blobs,
            )
        val response: HttpResponse<IngestResponse> =
            this.apiClient.api.postQueuedIngest(
                database = database,
                table = table,
                ingestRequest = ingestRequest,
            )
        return handleIngestResponse(
            response = response,
            database = database,
            table = table,
        )
    }

    override fun getMaxSourcesPerMultiIngest(): Int {
        // Get from configuration or return a default value
        return try {
            // TODO Get this from configuration ?
            // For now, return a reasonable default
            1000
        } catch (e: Exception) {
            logger.warn(
                "Failed to get max sources from configuration, using default",
                e,
            )
            1000
        }
    }

    override suspend fun ingestAsync(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties?,
    ): IngestResponse {
        when (source) {
            is BlobSource -> {
                return ingestAsync(listOf(source), database, table, props)
            }
            is LocalSource -> {
                // Upload the local source to blob storage
                val blobSource = uploader.uploadAsync(source)
                return ingestAsync(listOf(blobSource), database, table, props)
            }
            else -> {
                throw IngestClientException(
                    "Unsupported ingestion source type: ${source::class.simpleName}",
                )
            }
        }
    }

    override suspend fun getOperationSummaryAsync(
        operation: IngestionOperation,
    ): Status {
        TODO("Operation summary not yet implemented")
    }

    override suspend fun getOperationDetailsAsync(
        operation: IngestionOperation,
    ): StatusResponse {
        TODO("Operation details not yet implemented")
    }

    override fun close() {
        if (shouldDisposeUploader) {
            uploader.close()
        }
    }

    /**
     * Sanitizes a blob URL by removing the SAS token and query parameters to
     * allow proper duplicate detection.
     */
    private fun sanitizeBlobUrl(blobPath: String): String {
        return blobPath.split("?").first()
    }

    // Common way to parse ingestion response for both Streaming and Queued ingestion

    suspend fun <T : Any> handleIngestResponse(
        response: HttpResponse<T>,
        database: String,
        table: String,
    ): T {
        if (response.success) {
            val ingestResponseBody = response.body()
            return ingestResponseBody
        } else {
            if (response.status == HttpStatusCode.NotFound.value) {
                val message =
                    "Endpoint ${this.apiClient.dmUrl} not found. Please ensure that the " +
                        "target cluster is correct and reachable."
                logger.error(message)
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
                "Failed to submit Queued ingestion to $database.$table. " +
                    "Status: ${response.status}, Body: $nonSuccessResponseBody. " +
                    "OperationId $ingestResponseOperationId"
            logger.error(
                "Queued ingestion failed with response: {}",
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
