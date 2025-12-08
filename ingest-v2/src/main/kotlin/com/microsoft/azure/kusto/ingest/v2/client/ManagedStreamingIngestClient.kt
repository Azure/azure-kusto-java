// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client

import com.microsoft.azure.kusto.ingest.v2.STREAMING_MAX_REQ_BODY_SIZE
import com.microsoft.azure.kusto.ingest.v2.client.policy.ManagedStreamingErrorCategory
import com.microsoft.azure.kusto.ingest.v2.client.policy.ManagedStreamingPolicy
import com.microsoft.azure.kusto.ingest.v2.client.policy.ManagedStreamingRequestFailureDetails
import com.microsoft.azure.kusto.ingest.v2.client.policy.ManagedStreamingRequestSuccessDetails
import com.microsoft.azure.kusto.ingest.v2.common.RetryDecision
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestClientException
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.models.ExtendedIngestResponse
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestKind
import com.microsoft.azure.kusto.ingest.v2.common.models.IngestRequestPropertiesBuilder
import com.microsoft.azure.kusto.ingest.v2.common.runWithRetry
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.Status
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.BlobSource
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * Managed streaming ingestion client that combines streaming and queued
 * ingestion.
 *
 * This client intelligently chooses between streaming and queued ingestion
 * based on:
 * - Data size (falls back to queued for large data)
 * - Server response (falls back to queued on certain errors)
 * - Policy decisions (configured behavior)
 *
 * When streaming ingestion fails with transient errors, the client retries.
 * When it fails with certain permanent errors (e.g., streaming disabled, data
 * too large), it automatically falls back to queued ingestion.
 *
 * **Important:** This class cannot be instantiated directly. Use
 * [com.microsoft.azure.kusto.ingest.v2.builders.ManagedStreamingIngestClientBuilder]
 * to create instances of this client (to be implemented).
 *
 * Example usage:
 * ```
 * val client = ManagedStreamingIngestClientBuilder.create(clusterUrl)
 *     .withAuthentication(tokenProvider)
 *     .build()
 * ```
 */
class ManagedStreamingIngestClient
internal constructor(
    private val streamingIngestClient: StreamingIngestClient,
    private val queuedIngestClient: QueuedIngestClient,
    private val managedStreamingPolicy: ManagedStreamingPolicy,
) : IngestClient {

    private val logger =
        LoggerFactory.getLogger(ManagedStreamingIngestClient::class.java)

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
        requireNotNull(database.trim().isNotEmpty()) {
            "database cannot be blank"
        }
        requireNotNull(table.trim().isNotEmpty()) { "table cannot be blank" }

        val effectiveIngestRequestProperties =
            ingestRequestProperties
                ?: IngestRequestPropertiesBuilder(format = Format.csv)
                    .build()

        return when (source) {
            is BlobSource ->
                ingestBlobAsync(
                    source,
                    database,
                    table,
                    effectiveIngestRequestProperties,
                )
            is LocalSource ->
                ingestLocalAsync(
                    source,
                    database,
                    table,
                    effectiveIngestRequestProperties,
                )
            else ->
                throw IllegalArgumentException(
                    "Unsupported source type: ${source::class.simpleName}",
                )
        }
    }

    override suspend fun getOperationSummaryAsync(
        operation: IngestionOperation,
    ): Status {
        // Delegate to queued client for tracking
        if (operation.ingestKind ==  IngestKind.STREAMING) {
            logger.warn(
                "getOperationSummaryAsync called for a streaming ingestion operation. " +
                    "Streaming ingestion operations are not tracked. " +
                    "Returning a empty Status.",
            )
            return EMPTY_STATUS
        }
        return queuedIngestClient.getOperationSummaryAsync(operation)
    }

    override suspend fun getOperationDetailsAsync(
        operation: IngestionOperation,
    ): StatusResponse {
        // Delegate to queued client for tracking
        if (operation.ingestKind ==  IngestKind.STREAMING) {
            logger.warn(
                "getOperationDetailsAsync called for a streaming ingestion operation. " +
                        "Streaming ingestion operations are not tracked. " +
                        "Returning a dummy StatusResponse.",
            )
            return EMPTY_STATUS_RESPONSE
        }
        return queuedIngestClient.getOperationDetailsAsync(operation)
    }

    override fun close() {
        try {
            streamingIngestClient.close()
        } catch (e: Exception) {
            logger.warn("Error closing streaming ingest client", e)
        }
        try {
            queuedIngestClient.close()
        } catch (e: Exception) {
            logger.warn("Error closing queued ingest client", e)
        }
    }

    private suspend fun ingestBlobAsync(
        blobSource: BlobSource,
        database: String,
        table: String,
        ingestRequestProperties: IngestRequestProperties,
    ): ExtendedIngestResponse {
        if (
            shouldUseQueuedIngestByPolicy(
                blobSource,
                database,
                table,
                ingestRequestProperties,
            )
        ) {
            return invokeQueuedIngestionAsync(
                blobSource,
                database,
                table,
                ingestRequestProperties,
            )
        }
        return invokeStreamingIngestionAsync(
            blobSource,
            database,
            table,
            ingestRequestProperties,
        )
    }

    private suspend fun ingestLocalAsync(
        source: LocalSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): ExtendedIngestResponse {
        val stream = source.data()

        if (!stream.isValidForIngest()) {
            throw IngestClientException(
                message =
                "Stream is not valid for ingest. Ensure the stream is not null, has data, and is seekable.",
                isPermanent = true,
            )
        }

        val streamSize = withContext(Dispatchers.IO) {
            stream.available()
        }.toLong()

        if (
            shouldUseQueuedIngestBySize(streamSize) ||
            shouldUseQueuedIngestByPolicy(
                source,
                database,
                table,
                props,
            )
        ) {
            return invokeQueuedIngestionAsync(source, database, table, props)
        }

        return invokeStreamingIngestionAsync(source, database, table, props)
    }

    private fun shouldUseQueuedIngestBySize(size: Long): Boolean {
        val sizeThreshold =
            STREAMING_MAX_REQ_BODY_SIZE *
                managedStreamingPolicy.dataSizeFactor

        if (size > sizeThreshold) {
            logger.info(
                "Blob size '{}' is too big for streaming ingest. " +
                    "The DataSizeFactor used is '{}' - ingest using queued ingest.",
                size,
                managedStreamingPolicy.dataSizeFactor,
            )
            return true
        }
        return false
    }

    private suspend fun invokeStreamingIngestionAsync(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): ExtendedIngestResponse {
        var startTime: Long
        var currentAttempt = 1u
        var lastException: Exception? = null
        val result =
            managedStreamingPolicy.retryPolicy.runWithRetry(
                action = { attempt: UInt ->
                    startTime =
                        Instant.now(Clock.systemUTC())
                            .toEpochMilli()
                    currentAttempt = attempt
                    val result =
                        streamingIngestClient.ingestAsync(
                            source,
                            database,
                            table,
                            props,
                        )
                    val requestDuration =
                        Duration.ofMillis(
                            Instant.now(Clock.systemUTC())
                                .toEpochMilli() - startTime,
                        )
                    managedStreamingPolicy.streamingSuccessCallback(
                        source,
                        database,
                        table,
                        props,
                        ManagedStreamingRequestSuccessDetails(
                            requestDuration,
                        ),
                    )
                    result
                },
                onRetry = { _: UInt, _: Exception, _: Boolean ->
                    // Reset stream if possible for retry
                    resetLocalSourceIfPossible(source)
                },
                shouldRetry = {
                        _: UInt,
                        ex: Exception,
                        isPermanent: Boolean,
                    ->
                    lastException = ex
                    decideOnException(
                        source,
                        database,
                        table,
                        props,
                        isPermanent,
                        ex,
                    )
                },
                throwOnExhaustedRetries = false,
            )

        if (result != null) {
            return result
        }
        // Streaming failed, fall back to queued ingestion
        logger.warn(
            "Streaming ingestion failed, falling back to queued ingestion. Attempt: {}, Exception: {}",
            currentAttempt,
            lastException?.message,
        )
        return invokeQueuedIngestionAsync(source, database, table, props)
    }

    private fun resetLocalSourceIfPossible(source: IngestionSource) {
        if (source is LocalSource) {
            try {
                val stream = source.data()
                if (stream.markSupported()) {
                    stream.reset()
                }
            } catch (e: Exception) {
                logger.warn("Failed to reset stream for retry: {}", e.message)
            }
        }
    }

    private fun decideOnException(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        isPermanent: Boolean,
        ex: Exception,
    ): RetryDecision {
        if (!isPermanent) {
            reportTransientException(source, database, table, props, ex)
            return RetryDecision.Continue
        }

        val ingestEx = ex as? IngestException
        if (ingestEx == null) {
            reportUnknownException(source, database, table, props, ex)
            return RetryDecision.Throw
        }

        if (
            shouldFallbackToQueuedOnPermanentError(
                ingestEx,
                source,
                database,
                table,
                props,
            )
        ) {
            return RetryDecision.Break
        }
        logger.error(
            "Permanent error occurred while trying streaming ingest, didn't switch to queued according to policy: {}",
            ex.message,
            ex,
        )
        return RetryDecision.Throw
    }

    private suspend fun invokeQueuedIngestionAsync(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): ExtendedIngestResponse {
        return queuedIngestClient.ingestAsync(source, database, table, props)
    }

    private fun shouldUseQueuedIngestByPolicy(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): Boolean {
        if (
            managedStreamingPolicy.shouldDefaultToQueuedIngestion(
                source,
                database,
                table,
                props,
            )
        ) {
            logger.info(
                "According to the ManagedStreamingPolicy ingest will fall back to queued ingestion.",
            )
            return true
        }

        return false
    }

    private fun reportTransientException(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        ex: Exception,
    ) {
        val failureDetails =
            ManagedStreamingRequestFailureDetails(
                exception = ex,
                isPermanent = false,
                errorCategory =
                if (
                    (ex as? IngestException)?.failureCode ==
                    429
                ) {
                    ManagedStreamingErrorCategory.THROTTLED
                } else {
                    ManagedStreamingErrorCategory.OTHER_ERRORS
                },
            )

        logger.warn("Streaming ingestion throttled: {}", ex.message)
        managedStreamingPolicy.streamingErrorCallback(
            source,
            database,
            table,
            props,
            failureDetails,
        )
    }

    private fun reportUnknownException(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        ex: Exception,
    ) {
        logger.error("Unexpected error occurred during streaming ingestion", ex)

        managedStreamingPolicy.streamingErrorCallback(
            source,
            database,
            table,
            props,
            ManagedStreamingRequestFailureDetails(
                exception = ex,
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory.UNKNOWN_ERRORS,
            ),
        )
    }

    private fun shouldFallbackToQueuedOnPermanentError(
        ex: IngestException,
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): Boolean {
        val failureDetails =
            ManagedStreamingRequestFailureDetails(
                exception = ex,
                isPermanent = true,
            )

        // Check various error scenarios
        when {
            // Streaming ingestion policy turned off
            isStreamingIngestionOff(ex) -> {
                logger.info(
                    "Streaming ingestion is off, fallback to queued ingestion is {}, error: {}",
                    if (
                        managedStreamingPolicy
                            .continueWhenStreamingIngestionUnavailable
                    ) {
                        "on"
                    } else {
                        "off"
                    },
                    ex.message,
                )

                failureDetails.errorCategory =
                    ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF
                managedStreamingPolicy.streamingErrorCallback(
                    source,
                    database,
                    table,
                    props,
                    failureDetails,
                )

                return managedStreamingPolicy
                    .continueWhenStreamingIngestionUnavailable
            }

            // Table configuration prevents streaming
            isTableConfigPreventsStreaming(ex) -> {
                logger.info(
                    "Fallback to queued ingest due to a target table config, error: {}",
                    ex.message,
                )

                failureDetails.errorCategory =
                    ManagedStreamingErrorCategory
                        .TABLE_CONFIGURATION_PREVENTS_STREAMING
                managedStreamingPolicy.streamingErrorCallback(
                    source,
                    database,
                    table,
                    props,
                    failureDetails,
                )

                return true
            }

            // Request properties prevent streaming
            isRequestPropertiesPreventsStreaming(ex) -> {
                logger.info(
                    "Fallback to queued ingest due to request properties, error: {}",
                    ex.message,
                )

                failureDetails.errorCategory =
                    ManagedStreamingErrorCategory
                        .REQUEST_PROPERTIES_PREVENT_STREAMING
                managedStreamingPolicy.streamingErrorCallback(
                    source,
                    database,
                    table,
                    props,
                    failureDetails,
                )

                return true
            }

            else -> {
                logger.info(
                    "Don't fallback to queued ingest given this exception: {}",
                    ex.message,
                )

                failureDetails.errorCategory =
                    ManagedStreamingErrorCategory.OTHER_ERRORS
                managedStreamingPolicy.streamingErrorCallback(
                    source,
                    database,
                    table,
                    props,
                    failureDetails,
                )

                return false
            }
        }
    }

    private fun isStreamingIngestionOff(ex: IngestException): Boolean {
        // Check if error indicates streaming is disabled
        val message = ex.message.lowercase()
        return message.contains("streaming") &&
            (
                message.contains("disabled") ||
                    message.contains("not enabled") ||
                    message.contains("off")
                )
    }

    private fun isTableConfigPreventsStreaming(ex: IngestException): Boolean {
        // Check if error indicates table configuration prevents streaming
        val message = ex.message.lowercase()
        return message.contains("update policy") ||
            message.contains("schema") ||
            message.contains("incompatible")
    }

    private fun isRequestPropertiesPreventsStreaming(
        ex: IngestException,
    ): Boolean {
        // Check if error indicates request is too large or has incompatible properties
        val message = ex.message.lowercase()
        return message.contains("too large") ||
            message.contains("exceeds") ||
            message.contains("maximum allowed size") ||
            ex.failureCode == 413 // Request Entity Too Large
    }

    private fun InputStream.isValidForIngest(): Boolean {
        return try {
            this.available() > 0
        } catch (_: Exception) {
            false
        }
    }
}
