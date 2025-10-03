// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.utils.IngestionResultUtils
import com.microsoft.azure.kusto.ingest.v2.infrastructure.HttpResponse
import com.microsoft.azure.kusto.ingest.v2.models.Blob
import com.microsoft.azure.kusto.ingest.v2.models.BlobStatus
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequest
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.DataFormat
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import kotlin.time.Duration

class QueuedIngestionApiWrapper(
    override val dmUrl: String,
    override val tokenCredential: TokenCredential,
    override val skipSecurityChecks: Boolean = false,
) : KustoBaseApiClient(dmUrl, tokenCredential, skipSecurityChecks) {

    private val logger =
        LoggerFactory.getLogger(QueuedIngestionApiWrapper::class.java)

    /**
     * Submits a queued ingestion request.
     *
     * @param database The target database name
     * @param table The target table name
     * @param blobUrls List of blob URLs to ingest
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @return IngestionOperation for tracking the request
     */
    suspend fun submitQueuedIngestion(
        database: String,
        table: String,
        blobUrls: List<String>,
        format: DataFormat,
        ingestProperties: IngestRequestProperties? = null,
    ): IngestResponse {
        logger.info(
            "Submitting queued ingestion request for database: $database, table: $table, blobs: ${blobUrls.size}",
        )
        // Convert blob URLs to Blob objects
        val blobs =
            blobUrls.mapIndexed { index, url ->
                Blob(
                    url = url,
                    sourceId =
                    "source_${index}_${System.currentTimeMillis()}",
                )
            }

        // Use the domain model directly - no conversion needed!
        val apiFormat =
            Format.decode(format.kustoValue)
                ?: throw IngestException(
                    "Unsupported format: ${format.kustoValue}",
                )

        // Create IngestRequestProperties using simplified approach
        val requestProperties =
            ingestProperties ?: IngestRequestProperties(format = apiFormat)

        logger.info(
            "****************************************************************",
        )
        logger.info(
            "****************************************************************",
        )
        logger.info(
            "** Ingesting to $database.$table with the following properties:",
        )
        logger.info("** Format: $requestProperties")

        logger.info(
            "****************************************************************",
        )
        logger.info(
            "****************************************************************",
        )

        // Create the ingestion request
        val ingestRequest =
            IngestRequest(
                timestamp = OffsetDateTime.now(),
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

            if (response.success) {
                val ingestResponseBody = response.body()
                val operationId =
                    ingestResponseBody.ingestionOperationId
                        ?: throw IngestException(
                            "No operation ID returned from ingestion service",
                        )

                logger.info(
                    "Successfully submitted queued ingestion. Operation ID: $operationId",
                )
                logger.debug("Ingestion response: {}", response.body())

                return ingestResponseBody
            } else {
                // 404 is a special case - it indicates that the endpoint is not found. This may be
                // a transient network issue
                if (response.status == HttpStatusCode.NotFound.value) {
                    val message =
                        "Endpoint $dmUrl not found. Please ensure the cluster supports queued ingestion."
                    logger.error(
                        "Ingestion endpoint not found. Please ensure that the target cluster supports " +
                            "queued ingestion and that the endpoint URL is correct.",
                    )
                    throw IngestException(
                        message = message,
                        cause = RuntimeException(message),
                        failureCode = response.status,
                        failureSubCode = "",
                        isPermanent = false,
                    )
                }
                val nonSuccessResponseBody: IngestResponse = response.body()
                // Exception for non-success status codes except for 404 in which case you can retry
                val errorMessage =
                    "Failed to submit queued ingestion to $database.$table. " +
                        "Status: ${response.status}, Body: $nonSuccessResponseBody. " +
                        "OperationId ${nonSuccessResponseBody.ingestionOperationId}"
                logger.error("Ingestion failed with response: {}", errorMessage)
                throw IngestException(
                    message = errorMessage,
                    cause = RuntimeException(errorMessage),
                    isPermanent = true,
                )
            }
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
     * Gets a summary of the ingestion operation status (lightweight, fast).
     * This method provides overall status counters without detailed blob
     * information. Use this for quick status checks and polling scenarios.
     *
     * @param database The target database name
     * @param table The target table name
     * @param operationId The operation ID returned from the ingestion request
     * @return Updated IngestionOperation with status summary
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
                        if (hasTransientErrors) {
                            printMessagesFromFailures(transientFailures)
                        } else {
                            "Error polling $dmUrl for operation $operationId."
                        }
                    logger.error(message)
                    throw IngestException(
                        message = message,
                        cause = RuntimeException(message),
                        failureCode = response.status,
                        failureSubCode = "",
                        isPermanent = false,
                    )
                }
                val errorMessage =
                    printMessagesFromFailures(ingestStatusFailure.details)
                        ?: "Failed to get ingestion summary for operation $operationId. Status: ${response.status}, Body: $ingestStatusFailure"
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
                "FailureStatus ${failureStatus?.value}"
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
    suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean = false,
    ): StatusResponse {
        // If details are explicitly requested, use the details API
        if (forceDetails) {
            val statusResponse =
                getIngestionDetails(database, table, operationId, true)
            logger.info(
                "Forcing detailed status retrieval for operation: $operationId returning $statusResponse",
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
                    logger.debug(
                        "IngestionStatus: {}",
                        currentStatus.details,
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
