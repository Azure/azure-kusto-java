// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.ClientDetails
import com.microsoft.azure.kusto.ingest.v2.common.DefaultConfigurationCache
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.utils.IngestionResultUtils
import com.microsoft.azure.kusto.ingest.v2.container.BlobUploadContainer
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Blob
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequest
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import com.microsoft.azure.kusto.ingest.v2.source.SourceInfo
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import java.lang.Long
import java.time.Clock
import java.time.OffsetDateTime
import kotlin.time.Duration

class QueuedIngestionClient(
    override val dmUrl: String,
    override val tokenCredential: TokenCredential,
    override val skipSecurityChecks: Boolean = false,
    override val clientDetails: ClientDetails? = null,
    private val maxConcurrency: Int? = null,
    private val maxDataSize: kotlin.Long? = null,
    private val ignoreFileSize: Boolean = false,
) :
    KustoBaseApiClient(
        dmUrl,
        tokenCredential,
        skipSecurityChecks,
        clientDetails,
    ),
    IngestClient {

    override suspend fun submitIngestion(
        database: String,
        table: String,
        sources: List<com.microsoft.azure.kusto.ingest.v2.source.SourceInfo>,
        format: Format,
        ingestProperties: IngestRequestProperties?,
    ): IngestResponse {
        return submitQueuedIngestion(
            database = database,
            table = table,
            sources = sources,
            format = format,
            ingestProperties = ingestProperties,
            failOnPartialUploadError = true,
        )
    }

    override suspend fun getIngestionStatus(
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

    private val defaultConfigurationCache =
        DefaultConfigurationCache(
            dmUrl = dmUrl,
            tokenCredential = tokenCredential,
            skipSecurityChecks = skipSecurityChecks,
        )

    private val blobUploadContainer =
        BlobUploadContainer(
            configurationCache = defaultConfigurationCache,
            maxConcurrency =
            maxConcurrency ?: UPLOAD_CONTAINER_MAX_CONCURRENCY,
            maxDataSize =
            maxDataSize ?: UPLOAD_CONTAINER_MAX_DATA_SIZE_BYTES,
            ignoreSizeLimit = ignoreFileSize,
        )

    /**
     * Submits a queued ingestion request with support for all source types.
     * Local sources (FileSourceInfo, StreamSourceInfo) will be automatically
     * uploaded to blob storage before ingestion using parallel batch uploads.
     *
     * @param database The target database name
     * @param table The target table name
     * @param sources List of SourceInfo objects (BlobSourceInfo,
     *   FileSourceInfo, or StreamSourceInfo)
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @param failOnPartialUploadError If true, fails the entire operation if
     *   any uploads fail
     * @return IngestionOperation for tracking the request
     */
    suspend fun submitQueuedIngestion(
        database: String,
        table: String,
        sources: List<SourceInfo>,
        format: Format = Format.csv,
        ingestProperties: IngestRequestProperties? = null,
        failOnPartialUploadError: Boolean = true,
    ): IngestResponse {
        logger.info(
            "Submitting queued ingestion request for database: $database, table: $table, sources: ${sources.size}",
        )

        // Separate sources by type
        val blobSources = sources.filterIsInstance<BlobSourceInfo>()
        val localSources = sources.filterIsInstance<LocalSource>()

        // Convert local sources to blob sources
        val allBlobSources =
            if (localSources.isNotEmpty()) {
                logger.info(
                    "Uploading ${localSources.size} local sources to blob storage in parallel",
                )

                // Use batch upload for efficiency
                val batchResult =
                    BlobSourceInfo.fromLocalSourcesBatch(
                        localSources,
                        blobUploadContainer,
                    )

                // Log batch results
                logger.info(
                    "Batch upload completed: ${batchResult.successes.size} succeeded, " +
                        "${batchResult.failures.size} failed out of ${localSources.size} total",
                )

                // Handle failures based on policy
                if (batchResult.hasFailures) {
                    val failureDetails =
                        batchResult.failures.joinToString("\n") {
                                failure ->
                            "  - ${failure.source.name}: ${failure.errorCode} - ${failure.errorMessage}"
                        }

                    if (failOnPartialUploadError) {
                        throw IngestException(
                            "Failed to upload ${batchResult.failures.size} out of ${localSources.size} sources:\n$failureDetails",
                            isPermanent =
                            batchResult.failures.all {
                                it.isPermanent
                            },
                        )
                    } else {
                        logger.warn(
                            "Some uploads failed but continuing with successful uploads:\n$failureDetails",
                        )
                    }
                }

                blobSources + batchResult.successes
            } else {
                blobSources
            }

        if (allBlobSources.isEmpty()) {
            throw IngestException(
                "No sources available for ingestion after upload processing",
                isPermanent = true,
            )
        }
        // Convert BlobSourceInfo objects to Blob objects
        val blobs =
            allBlobSources.map { blobSource ->
                val sourceId = blobSource.sourceId.toString()
                Blob(
                    url = blobSource.blobPath,
                    sourceId = sourceId,
                    rawSize = blobSource.blobExactSize as Long?,
                )
            }

        val requestProperties =
            ingestProperties ?: IngestRequestProperties(format = format)

        logger.debug(
            "Ingesting to {}.{} with the following properties with properties {}",
            database,
            table,
            requestProperties,
        )

        val ingestRequest =
            IngestRequest(
                timestamp = OffsetDateTime.now(Clock.systemUTC()),
                blobs = blobs,
                properties = requestProperties,
            )

        try {
            val response: HttpResponse<IngestResponse> =
                api.postQueuedIngest(
                    database = database,
                    table = table,
                    ingestRequest = ingestRequest,
                )

            return handleIngestResponse(
                response = response,
                database = database,
                table = table,
                dmUrl = dmUrl,
                endpointType = "queued",
            )
        } catch (e: Exception) {
            logger.error(
                "Exception occurred during queued ingestion submission",
                e,
            )
            if (e is IngestException) throw e
            throw IngestException(
                message =
                "Error submitting queued ingest request to $dmUrl",
                cause = e,
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
    override suspend fun getIngestionDetails(
        database: String,
        table: String,
        operationId: String,
        details: Boolean,
    ): StatusResponse {
        logger.debug("Checking ingestion summary for operation: $operationId")
        try {
            val response: HttpResponse<StatusResponse> =
                api.getIngestStatus(
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
}
