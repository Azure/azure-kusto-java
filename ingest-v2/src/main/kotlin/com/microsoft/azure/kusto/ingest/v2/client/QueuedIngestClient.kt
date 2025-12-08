// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.KustoBaseApiClient
import com.microsoft.azure.kusto.ingest.v2.MAX_BLOBS_PER_BATCH
import com.microsoft.azure.kusto.ingest.v2.common.ConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestClientException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestSizeLimitExceededException
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.utils.IngestionResultUtils
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Blob
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
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
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.time.Clock
import java.time.OffsetDateTime
import kotlin.time.Duration

/**
 * Queued ingestion client for Azure Data Explorer (Kusto).
 *
 * This client handles ingestion through the queued ingestion path, which
 * provides reliable, asynchronous data ingestion with operation tracking
 * capabilities.
 *
 * **Important:** This class cannot be instantiated directly. Use
 * [com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestClientBuilder] to
 * create instances of this client. The internal constructor ensures that only
 * the builder can create properly configured instances.
 *
 * Example usage:
 * ```
 * val client = QueuedIngestionClientBuilder.create(dmEndpoint)
 *     .withAuthentication(tokenProvider)
 *     .withMaxConcurrency(10)
 *     .build()
 * ```
 *
 * @see
 *   com.microsoft.azure.kusto.ingest.v2.builders.QueuedIngestionClientBuilder
 */
class QueuedIngestClient
internal constructor(
    private val apiClient: KustoBaseApiClient,
    private val cachedConfiguration: ConfigurationCache,
    private val uploader: IUploader,
    private val shouldDisposeUploader: Boolean = false,
) : MultiIngestClient {
    private val logger = LoggerFactory.getLogger(QueuedIngestClient::class.java)

    override suspend fun ingestAsync(
        sources: List<IngestionSource>,
        database: String,
        table: String,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse {
        // Validate sources list is not empty
        require(sources.isNotEmpty()) { "sources list cannot be empty" }
        val maxBlobsPerBatch = getMaxSourcesPerMultiIngest()
        // Check if sources count exceeds the limit
        if (sources.size > maxBlobsPerBatch) {
            throw IngestSizeLimitExceededException(
                size = sources.size.toLong(),
                maxNumberOfBlobs = maxBlobsPerBatch,
                message =
                "Ingestion sources count(${sources.size}) is larger than the limit allowed ($maxBlobsPerBatch)",
                isPermanent = true,
            )
        }

        // Check that all blobs have the same format
        val differentFormatBlob =
            sources.map { source -> source.format }.toSet()
        if (differentFormatBlob.size > 1) {
            logger.error(
                "All blobs in the request must have the same format.Received formats [{}]",
                differentFormatBlob.joinToString(", "),
            )
            throw IngestClientException(
                "All blobs in the request must have the same format. All blobs in the request must have the same format.Received formats: $differentFormatBlob",
            )
        }

        // Split sources and upload local sources in parallel
        val blobSources = uploadLocalSourcesAsync(sources)

        // Check for duplicate blob URLs
        val duplicates =
            blobSources
                .groupBy { sanitizeBlobUrl(it.blobPath) }
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
            blobSources.map {
                Blob(
                    it.blobPath,
                    sourceId = it.sourceId.toString(),
                    rawSize = it.blobExactSize,
                )
            }
        val ingestRequest =
            IngestRequest(
                timestamp = OffsetDateTime.now(Clock.systemUTC()),
                blobs = blobs,
                properties = ingestRequestProperties,
            )
        val response: HttpResponse<IngestResponse> =
            this.apiClient.api.postQueuedIngest(
                database = database,
                table = table,
                ingestRequest = ingestRequest,
            )
        val ingestResponse =
            handleIngestResponse(
                response = response,
                database = database,
                table = table,
            )
        return ExtendedIngestResponse(ingestResponse, IngestKind.QUEUED)
    }

    override suspend fun getMaxSourcesPerMultiIngest(): Int {
        // Get from configuration or return a default value
        return try {
            return cachedConfiguration
                .getConfiguration()
                .ingestionSettings
                ?.maxBlobsPerBatch
                ?.toInt() ?: MAX_BLOBS_PER_BATCH
        } catch (e: Exception) {
            logger.warn(
                "Failed to get max sources from configuration, using default",
                e,
            )
            MAX_BLOBS_PER_BATCH
        }
    }

    override suspend fun ingestAsync(
        source: IngestionSource,
        database: String,
        table: String,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse {
        // Add this as a fallback because the format is mandatory and if that is not present it may
        // cause a failure
        val effectiveIngestionProperties =
            ingestRequestProperties
                ?: IngestRequestPropertiesBuilder(format = Format.csv)
                    .build()
        when (source) {
            is BlobSource -> {
                return ingestAsync(
                    listOf(source),
                    database,
                    table,
                    effectiveIngestionProperties,
                )
            }
            is LocalSource -> {
                // Upload the local source to blob storage
                val blobSource = uploader.uploadAsync(source)
                return ingestAsync(
                    listOf(blobSource),
                    database,
                    table,
                    effectiveIngestionProperties,
                )
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
        val statusResponse =
            getIngestionDetails(
                operation.database,
                operation.table,
                operation.operationId.toString(),
                false,
            )
        return statusResponse.status
                ?: Status(
                    inProgress = 0,
                    succeeded = 0,
                    failed = 0,
                    canceled = 0,
                )
    }

    override suspend fun getOperationDetailsAsync(
        operation: IngestionOperation,
    ): StatusResponse {
        return getIngestionDetails(
                database = operation.database,
                table = operation.table,
                operationId = operation.operationId.toString(),
                details = true,
            )
    }

    override fun close() {
        if (shouldDisposeUploader) {
            uploader.close()
        }
    }

    /**
     * Splits sources into BlobSources and LocalSources, uploads LocalSources in
     * parallel, and returns a unified list of BlobSources.
     *
     * @param sources The list of ingestion sources to process
     * @return A list of BlobSources including both original BlobSources and
     *   uploaded LocalSources
     * @throws IngestClientException if an unsupported source type is
     *   encountered
     */
    private suspend fun uploadLocalSourcesAsync(
        sources: List<IngestionSource>,
    ): List<BlobSource> {
        // Split sources into BlobSources and LocalSources
        val blobSources = mutableListOf<BlobSource>()
        val localSources = mutableListOf<LocalSource>()

        sources.forEach { source ->
            when (source) {
                is BlobSource -> blobSources.add(source)
                is LocalSource -> localSources.add(source)
                else ->
                    throw IngestClientException(
                        "Unsupported ingestion source type: ${source::class.simpleName}",
                    )
            }
        }

        // Upload LocalSources in parallel and collect the resulting BlobSources
        if (localSources.isNotEmpty()) {
            logger.info(
                "Uploading ${localSources.size} local source(s) to blob storage",
            )
            val uploadedBlobs = coroutineScope {
                localSources
                    .map { localSource ->
                        async { uploader.uploadAsync(localSource) }
                    }
                    .awaitAll()
            }
            blobSources.addAll(uploadedBlobs)
            logger.info(
                "Successfully uploaded ${uploadedBlobs.size} local source(s)",
            )
        }

        return blobSources
    }

    /**
     * Sanitizes a blob URL by removing the SAS token and query parameters to
     * allow proper duplicate detection.
     */
    private fun sanitizeBlobUrl(blobPath: String): String {
        return blobPath.split("?").first()
    }

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

    /**
     * Gets detailed information about an ingestion operation.
     *
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from the ingestion request
     * @param details Whether to retrieve detailed blob-level information
     * @return StatusResponse with operation details
     */
    private suspend fun getIngestionDetails(
        database: String,
        table: String,
        operationId: String,
        details: Boolean,
    ): StatusResponse {
        logger.debug("Checking ingestion summary for operation: $operationId")
        try {
            val response: HttpResponse<StatusResponse> =
                this.apiClient.api.getIngestStatus(
                    database = database,
                    table = table,
                    operationId = operationId,
                    details = details,
                )

            if (
                response.success &&
                response.status == HttpStatusCode.OK.value
            ) {
                val ingestStatusResponse = response.body()
                logger.debug(
                    "Successfully retrieved summary for operation: {} and details: {}",
                    operationId,
                    ingestStatusResponse,
                )
                return ingestStatusResponse
            } else {
                logger.error(response.toString())
                val ingestStatusFailure: StatusResponse = response.body()
                // check if it is a permanent failure from status
                val transientFailures =
                    ingestStatusFailure.details?.filter {
                        it.failureStatus ==
                            BlobStatus.FailureStatus.Transient
                    }
                val hasTransientErrors = transientFailures.isNullOrEmpty()

                if (
                    response.status == HttpStatusCode.NotFound.value ||
                    hasTransientErrors
                ) {
                    val message =
                        printMessagesFromFailures(
                            transientFailures,
                            isTransientFailure = true,
                        )
                    logger.error(message)
                    throw IngestException(
                        message = message,
                        cause = RuntimeException(message),
                        failureCode = response.status,
                        failureSubCode = "",
                        isPermanent = false,
                    )
                }
                // TODO: We need to eventually look at OneApiExceptions
                val errorMessage =
                    printMessagesFromFailures(
                        ingestStatusFailure.details,
                        isTransientFailure = false,
                    )
                logger.error(errorMessage)
                throw IngestException(errorMessage, isPermanent = true)
            }
        } catch (e: Exception) {
            logger.error(
                "Exception occurred while getting ingestion summary for operation: $operationId",
                e,
            )
            if (e is IngestException) throw e
            throw IngestException(
                "Failed to get ingestion summary: ${e.message}",
                e,
            )
        }
    }

    private suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean,
    ): StatusResponse {
        // If details are explicitly requested, use the details API
        if (forceDetails) {
            val statusResponse =
                getIngestionDetails(database, table, operationId, true)
            logger.debug(
                "Forcing detailed status retrieval for operation: {} returning {}",
                operationId,
                statusResponse,
            )
            return statusResponse
        }
        // Start with summary for efficiency
        val statusResponse =
            getIngestionDetails(database, table, operationId, false)
        // If operation has failures or is completed, get detailed information
        return if (
            statusResponse.status?.failed?.let { it > 0 } == true ||
            IngestionResultUtils.isCompleted(statusResponse.details)
        ) {
            logger.debug(
                "Operation $operationId has failures or is completed, retrieving details",
            )
            getIngestionDetails(database, table, operationId, true)
        } else {
            statusResponse
        }
    }

    /**
     * Polls the ingestion status until completion or timeout.
     *
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID to poll
     * @param pollingInterval How often to check the status
     * @param timeout Maximum time to wait before throwing timeout exception
     * @return The final StatusResponse when ingestion is completed
     * @throws IngestException if the operation times out or fails
     */
    suspend fun pollUntilCompletion(
        database: String,
        table: String,
        operationId: String,
        pollingInterval: Duration = Duration.parse("PT30S"),
        timeout: Duration = Duration.parse("PT5M"),
    ): StatusResponse {
        val result =
            withTimeoutOrNull(timeout.inWholeMilliseconds) {
                var currentStatus: StatusResponse
                do {
                    currentStatus =
                        getIngestionStatus(
                            database,
                            table,
                            operationId,
                            forceDetails = true,
                        )
                    logger.debug(
                        "Starting to poll ingestion status for operation: $operationId, timeout: $timeout",
                    )
                    if (
                        IngestionResultUtils.isCompleted(
                            currentStatus.details,
                        )
                    ) {
                        logger.info(
                            "Ingestion operation $operationId completed",
                        )
                        return@withTimeoutOrNull currentStatus
                    }

                    logger.debug(
                        "Ingestion operation $operationId still in progress, waiting ${pollingInterval.inWholeSeconds}s before next check",
                    )
                    delay(pollingInterval.inWholeMilliseconds)
                } while (
                    !IngestionResultUtils.isCompleted(
                        currentStatus.details,
                    )
                )

                currentStatus
            }

        return result
            ?: throw IngestException(
                "Ingestion operation $operationId timed out after $timeout. " +
                    "Consider increasing the timeout duration or check the operation status manually.",
            )
    }

    private fun printMessagesFromFailures(
        failures: List<BlobStatus>?,
        isTransientFailure: Boolean,
    ): String? {
        return failures?.joinToString {
                (
                    sourceId,
                    status,
                    startedAt,
                    lastUpdateTime,
                    errorCode,
                    failureStatus,
                    details,
                ),
            ->
            "Error ingesting blob with $sourceId. ErrorDetails $details, ErrorCode $errorCode " +
                ", Status ${status?.value}. Ingestion lastUpdated at $lastUpdateTime & started at $startedAt. " +
                "FailureStatus ${failureStatus?.value}. Is transient failure: $isTransientFailure"
        }
    }
}
