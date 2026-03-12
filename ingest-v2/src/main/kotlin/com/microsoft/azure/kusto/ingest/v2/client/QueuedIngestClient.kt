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
import com.microsoft.azure.kusto.ingest.v2.common.models.withFormatFromSource
import com.microsoft.azure.kusto.ingest.v2.common.utils.IngestionResultUtils
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Blob
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequest
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.uploader.IUploader
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.future
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.ConnectException
import java.time.Clock
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.CompletableFuture

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

    /**
     * Ingests data from multiple blob sources with the given properties. This
     * is the suspend function for Kotlin callers.
     *
     * Multi-blob ingestion only supports [BlobSource]. The blobs are assumed to
     * already exist in blob storage, so no upload is performed - the request is
     * sent directly to the Data Management service.
     */
    override suspend fun ingestAsync(
        database: String,
        table: String,
        sources: List<BlobSource>,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse =
        ingestAsyncInternal(
            database,
            table,
            sources,
            ingestRequestProperties,
        )

    /**
     * Ingests data from a single source with the given properties. This is the
     * suspend function for Kotlin callers.
     */
    override suspend fun ingestAsync(
        database: String,
        table: String,
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse =
        ingestAsyncSingleInternal(
            database,
            table,
            source,
            ingestRequestProperties,
        )

    /**
     * Ingests data from multiple blob sources with the given properties. This
     * is the Java-friendly version that returns a CompletableFuture.
     *
     * Multi-blob ingestion only supports [BlobSource]. The blobs are assumed to
     * already exist in blob storage, so no upload is performed - the request is
     * sent directly to the Data Management service.
     */
    override fun ingestAsyncJava(
        database: String,
        table: String,
        sources: List<BlobSource>,
        ingestRequestProperties: IngestRequestProperties?,
    ): CompletableFuture<ExtendedIngestResponse> =
        CoroutineScope(Dispatchers.IO).future {
            ingestAsyncInternal(
                database,
                table,
                sources,
                ingestRequestProperties,
            )
        }

    /**
     * Ingests data from a single source with the given properties. This is the
     * Java-friendly version that returns a CompletableFuture.
     */
    override fun ingestAsyncJava(
        database: String,
        table: String,
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties?,
    ): CompletableFuture<ExtendedIngestResponse> =
        CoroutineScope(Dispatchers.IO).future {
            ingestAsyncSingleInternal(
                database,
                table,
                source,
                ingestRequestProperties,
            )
        }

    /**
     * Gets the operation summary for the specified ingestion operation. This is
     * the Java-friendly version that returns a CompletableFuture.
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
     */
    override fun getOperationDetailsAsyncJava(
        operation: IngestionOperation,
    ): CompletableFuture<StatusResponse> =
        CoroutineScope(Dispatchers.IO).future {
            getOperationDetailsAsync(operation)
        }

    /**
     * Returns the maximum number of sources that can be ingested in a single
     * call to [ingestAsync]. This is the Java-friendly version that returns a
     * CompletableFuture.
     */
    override fun getMaxSourcesPerMultiIngestJava(): CompletableFuture<Int> =
        CoroutineScope(Dispatchers.IO).future {
            getMaxSourcesPerMultiIngest()
        }

    /**
     * Polls the ingestion status until completion or timeout. This is the
     * Java-friendly version that returns a CompletableFuture.
     *
     * @param operation The ingestion operation to poll
     * @param timeout Maximum time to wait before throwing timeout exception (in
     *   milliseconds)
     * @return CompletableFuture that completes with the final StatusResponse
     *   when ingestion is completed
     */
    fun pollForCompletion(
        operation: IngestionOperation,
        pollingInterval: Duration,
        timeout: Duration,
    ): CompletableFuture<StatusResponse> =
        CoroutineScope(Dispatchers.IO).future {
            pollUntilCompletion(
                database = operation.database,
                table = operation.table,
                operationId = operation.operationId,
                pollingInterval = pollingInterval,
                timeout = timeout,
            )
        }

    /**
     * Internal implementation of ingestAsync for multiple blob sources.
     *
     * This method only accepts [BlobSource] - no upload is performed. The blobs
     * are assumed to already exist in blob storage.
     */
    private suspend fun ingestAsyncInternal(
        database: String,
        table: String,
        sources: List<BlobSource>,
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
                message =
                "All blobs in the request must have the same format. Received formats: $differentFormatBlob",
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
                message =
                "Duplicate blob sources detected in the request: [$duplicateInfo]",
            )
        }

        // Create blob objects for the request
        val blobs =
            sources.map {
                Blob(
                    it.blobPath,
                    sourceId = it.sourceId.toString(),
                    rawSize = it.blobExactSize,
                )
            }

        // Extract format from the first source (all sources have same format as validated above)
        val effectiveProperties =
            ingestRequestProperties?.withFormatFromSource(sources.first())
                ?: IngestRequestPropertiesBuilder.create()
                    .build()
                    .withFormatFromSource(sources.first())

        val ingestRequest =
            IngestRequest(
                timestamp = OffsetDateTime.now(Clock.systemUTC()),
                blobs = blobs,
                properties = effectiveProperties,
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

    /** Internal implementation of ingestAsync for a single source. */
    private suspend fun ingestAsyncSingleInternal(
        database: String,
        table: String,
        source: IngestionSource,
        ingestRequestProperties: IngestRequestProperties?,
    ): ExtendedIngestResponse {
        when (source) {
            is BlobSource -> {
                // Pass the source to multi-source method which will extract format
                return ingestAsync(
                    database,
                    table,
                    listOf(source),
                    ingestRequestProperties,
                )
            }
            is LocalSource -> {
                // Upload the local source to blob storage, then ingest
                // Note: We pass the original LocalSource to preserve format information
                val blobSource = uploader.uploadAsync(source)
                // Use the original source's format
                return ingestAsync(
                    database,
                    table,
                    listOf(blobSource),
                    ingestRequestProperties,
                )
            }
            else -> {
                throw IngestClientException(
                    message =
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
                operation.operationId,
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
            operationId = operation.operationId,
            details = true,
        )
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
            withTimeoutOrNull(timeout.toMillis()) {
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
                        "Starting to poll ingestion status for operation: {}, timeout: {}",
                        operationId,
                        timeout,
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
                        "Ingestion operation $operationId still in progress, waiting ${pollingInterval.toMillis()}s before next check",
                    )
                    delay(pollingInterval.toMillis())
                } while (
                    !IngestionResultUtils.isCompleted(
                        currentStatus.details,
                    )
                )

                currentStatus
            }

        if (result != null) {
            logger.debug(
                "Finished polling ingestion status for operation: {}, result: {}",
                operationId,
                result,
            )
            result.status?.failed?.let {
                if (it >= 1) {
                    val errorMessage =
                        printMessagesFromFailures(
                            result.details,
                            isTransientFailure = false,
                        )
                    logger.error(
                        "Ingestion operation $operationId failed. $errorMessage",
                    )
                    throw IngestException(
                        "Ingestion operation $operationId failed. $errorMessage",
                        isPermanent = true,
                    )
                }
            }
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
            buildString {
                append("Error ingesting blob with $sourceId. ")
                if (!details.isNullOrBlank()) append("ErrorDetails: $details. ")
                if (!errorCode.isNullOrBlank()) {
                    append("ErrorCode: $errorCode. ")
                }
                if (status != null) append("Status: ${status.value}. ")
                if (lastUpdateTime != null) {
                    append("Ingestion lastUpdated at $lastUpdateTime. ")
                }
                if (startedAt != null) append("Started at $startedAt. ")
                if (failureStatus != null) {
                    append("FailureStatus: ${failureStatus.value}. ")
                }
                append("IsTransientFailure: $isTransientFailure")
            }
        }
    }
}
