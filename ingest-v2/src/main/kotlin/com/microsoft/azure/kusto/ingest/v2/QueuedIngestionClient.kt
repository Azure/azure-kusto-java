// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
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
import com.microsoft.azure.kusto.ingest.v2.source.AbstractSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.FileSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.StreamSourceInfo
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
) :
    KustoBaseApiClient(dmUrl, tokenCredential, skipSecurityChecks),
    IngestClient {

    override suspend fun submitIngestion(
        database: String,
        table: String,
        sources: List<com.microsoft.azure.kusto.ingest.v2.source.SourceInfo>,
        format: Format,
        ingestProperties: IngestRequestProperties?,
    ): IngestResponse {
        val abstractSources = sources.map { it as AbstractSourceInfo }
        return submitQueuedIngestion(
            database,
            table,
            abstractSources,
            format,
            ingestProperties,
        )
    }

    override suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean,
    ): StatusResponse {
        return getIngestionStatusInternal(
            database,
            table,
            operationId,
            forceDetails,
        )
    }

    private val defaultConfigurationCache =
        DefaultConfigurationCache(
            dmUrl = dmUrl,
            tokenCredential = tokenCredential,
            skipSecurityChecks = skipSecurityChecks,
        )

    // Reusable BlobUploadContainer instance
    private val blobUploadContainer =
        BlobUploadContainer(defaultConfigurationCache)

    /**
     * Submits a queued ingestion request with support for all source types.
     * Local sources (FileSourceInfo, StreamSourceInfo) will be automatically
     * uploaded to blob storage before ingestion.
     *
     * @param database The target database name
     * @param table The target table name
     * @param sources List of SourceInfo objects (BlobSourceInfo,
     *   FileSourceInfo, or StreamSourceInfo)
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @return IngestionOperation for tracking the request
     */
    suspend fun submitQueuedIngestion(
        database: String,
        table: String,
        sources: List<AbstractSourceInfo>,
        format: Format = Format.csv,
        ingestProperties: IngestRequestProperties? = null,
    ): IngestResponse {
        logger.info(
            "Submitting queued ingestion request for database: $database, table: $table, sources: ${sources.size}",
        )
        // Convert all sources to BlobSourceInfo
        val blobSources =
            sources.map { source ->
                when (source) {
                    is BlobSourceInfo -> source
                    is FileSourceInfo -> {
                        logger.debug(
                            "Uploading FileSourceInfo to blob storage: ${source.name}",
                        )
                        BlobSourceInfo.fromFileSourceInfo(
                            source,
                            blobUploadContainer,
                        )
                    }
                    is StreamSourceInfo -> {
                        logger.debug(
                            "Uploading StreamSourceInfo to blob storage: ${source.name}",
                        )
                        BlobSourceInfo.fromStreamSourceInfo(
                            source,
                            blobUploadContainer,
                        )
                    }
                    else ->
                        throw IngestException(
                            "Unsupported source type: ${source::class.simpleName}",
                            isPermanent = true,
                        )
                }
            }
        // Convert BlobSourceInfo objects to Blob objects
        val blobs =
            blobSources.map { blobSource ->
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
     * Gets the status of a queued ingestion operation with intelligent API
     * selection. For completed operations or when details are explicitly
     * requested, uses the details API. For in-progress operations, uses the
     * summary API for efficiency.
     *
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from the ingestion request
     * @param forceDetails Force retrieval of detailed information regardless of
     *   operation status
     * @return Updated IngestionOperation with current status
     */
    private suspend fun getIngestionStatusInternal(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean = false,
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
}
