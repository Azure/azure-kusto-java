// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.azure.core.credential.TokenCredential
import com.microsoft.azure.kusto.ingest.v2.common.Retry
import com.microsoft.azure.kusto.ingest.v2.common.exceptions.IngestException
import com.microsoft.azure.kusto.ingest.v2.common.runWithRetry
import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.models.IngestResponse
import com.microsoft.azure.kusto.ingest.v2.models.StatusResponse
import com.microsoft.azure.kusto.ingest.v2.source.AbstractSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.BlobSourceInfo
import com.microsoft.azure.kusto.ingest.v2.source.LocalSource
import io.ktor.http.HttpStatusCode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * ManagedStreamingIngestClient chooses between streaming and queued ingestion
 * based on data size, service availability, and error patterns.
 *
 * This client:
 * - Attempts streaming ingestion first for eligible data
 * - Automatically falls back to queued ingestion on failures
 * - Implements retry logic with exponential backoff
 * - Tracks per-table error patterns to optimize future ingestion attempts
 * - Respects streaming ingestion limits and policies
 *
 * @param clusterUrl The Kusto cluster URL (used for both streaming and queued
 *   ingestion)
 * @param tokenCredential Azure credential for authentication
 * @param managedStreamingPolicy Policy controlling fallback behavior and retry
 *   logic
 * @param skipSecurityChecks Whether to skip security checks (for testing)
 */
class ManagedStreamingIngestClient(
    private val clusterUrl: String,
    private val tokenCredential: TokenCredential,
    private val managedStreamingPolicy: ManagedStreamingPolicy =
        DefaultManagedStreamingPolicy(),
    private val skipSecurityChecks: Boolean = false,
) : IngestClient {

    override val logger: Logger =
        LoggerFactory.getLogger(ManagedStreamingIngestClient::class.java)

    private val streamingIngestClient =
        StreamingIngestClient(
            engineUrl = clusterUrl,
            tokenCredential = tokenCredential,
            skipSecurityChecks = skipSecurityChecks,
        )

    private val queuedIngestionClient =
        QueuedIngestionClient(
            dmUrl = clusterUrl,
            tokenCredential = tokenCredential,
            skipSecurityChecks = skipSecurityChecks,
        )

    // Maximum size for streaming ingestion (4MB default, can be tuned with dataSizeFactor)
    private val maxStreamingIngestSize: Long =
        (4 * 1024 * 1024 * managedStreamingPolicy.dataSizeFactor).toLong()

    /**
     * Submits an ingestion request, intelligently choosing between streaming
     * and queued ingestion.
     *
     * @param database The target database name
     * @param table The target table name
     * @param sources List of SourceInfo objects (BlobSourceInfo,
     *   FileSourceInfo, or StreamSourceInfo)
     * @param format The data format
     * @param ingestProperties Optional ingestion properties
     * @return IngestResponse for tracking the request
     */
    suspend fun submitManagedIngestion(
        database: String,
        table: String,
        sources: List<AbstractSourceInfo>,
        format: Format = Format.csv,
        ingestProperties: IngestRequestProperties? = null,
    ): IngestResponse {
        require(database.isNotBlank()) { "Database name cannot be blank" }
        require(table.isNotBlank()) { "Table name cannot be blank" }
        require(sources.isNotEmpty()) { "Sources list cannot be empty" }

        val props = ingestProperties ?: IngestRequestProperties(format = format)

        logger.info(
            "Starting managed ingestion for database: $database, table: $table, sources: ${sources.size}",
        )

        // Process each source
        for (source in sources) {
            when (source) {
                is BlobSourceInfo -> ingestBlob(source, database, table, props)
                is LocalSource -> ingestLocal(source, database, table, props)
                else ->
                    throw IngestException(
                        "Unsupported source type: ${source::class.simpleName}",
                        isPermanent = true,
                    )
            }
        }

        // Return a combined response (for now, return success)
        return IngestResponse(
            ingestionOperationId = "managed-${System.currentTimeMillis()}",
        )
    }

    private suspend fun ingestBlob(
        source: BlobSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): IngestResponse {
        if (shouldUseQueuedIngestByPolicy(source, database, table, props)) {
            logger.info(
                "Policy dictates using queued ingestion for blob: ${source.blobPath}",
            )
            return invokeQueuedIngestion(source, database, table, props)
        }

        return invokeStreamingIngestion(source, database, table, props)
    }

    private suspend fun ingestLocal(
        source: LocalSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): IngestResponse {
        val stream = source.data()

        if (!isStreamValid(stream)) {
            throw IngestException(
                "Stream is not valid for ingest. Ensure the stream is not null, has data, and is seekable.",
                isPermanent = true,
            )
        }

        if (shouldUseQueuedIngestBySize(stream, source)) {
            logger.info(
                "Data size exceeds streaming limit, using queued ingestion",
            )
            return invokeQueuedIngestion(source, database, table, props)
        }

        if (shouldUseQueuedIngestByPolicy(source, database, table, props)) {
            logger.info(
                "Policy dictates using queued ingestion for local source: ${source.name}",
            )
            return invokeQueuedIngestion(source, database, table, props)
        }

        return invokeStreamingIngestion(source, database, table, props)
    }

    private fun isStreamValid(stream: InputStream): Boolean {
        return try {
            // Mark the current position if supported
            if (stream.markSupported()) {
                stream.mark(1)
                val hasData = stream.read() != -1
                stream.reset() // Reset to marked position
                hasData
            } else {
                // For non-markable streams, check available bytes
                stream.available() > 0
            }
        } catch (e: Exception) {
            logger.warn("Stream validation failed: ${e.message}")
            false
        }
    }

    private fun shouldUseQueuedIngestBySize(
        stream: InputStream,
        source: LocalSource,
    ): Boolean {
        val size = source.size()

        if (size == null) {
            logger.warn("Could not determine data size for ${source::class.simpleName}")
            return false
        }

        if (size > maxStreamingIngestSize) {
            logger.info(
                "Data size '$size' exceeds streaming limit '$maxStreamingIngestSize'. " +
                    "DataSizeFactor used: ${managedStreamingPolicy.dataSizeFactor}. Using queued ingestion.",
            )
            return true
        }

        return false
    }

    private fun shouldUseQueuedIngestByPolicy(
        source: AbstractSourceInfo,
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
                "ManagedStreamingPolicy indicates fallback to queued ingestion",
            )
            return true
        }
        return false
    }

    private suspend fun invokeStreamingIngestion(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): IngestResponse {
        val startTime = System.currentTimeMillis()

        val result =
            managedStreamingPolicy.retryPolicy.runWithRetry(
                action = { attempt ->
                    val attemptStartTime = System.currentTimeMillis()

                    try {
                        val response =
                            when (source) {
                                is BlobSourceInfo -> {
                                    streamingIngestClient
                                        .submitStreamingIngestion(
                                            database =
                                            database,
                                            table = table,
                                            // Not used for blob-based streaming
                                            data =
                                            ByteArray(
                                                0,
                                            ),
                                            format =
                                            props
                                                .format,
                                            ingestProperties =
                                            props,
                                            blobUrl =
                                            source
                                                .blobPath,
                                        )
                                    IngestResponse(
                                        ingestionOperationId =
                                        source.sourceId
                                            .toString(),
                                    )
                                }

                                is LocalSource -> {
                                    val data =
                                        source.data()
                                            .readBytes()
                                    streamingIngestClient
                                        .submitStreamingIngestion(
                                            database =
                                            database,
                                            table = table,
                                            data = data,
                                            format =
                                            props
                                                .format,
                                            ingestProperties =
                                            props,
                                        )
                                    IngestResponse(
                                        ingestionOperationId =
                                        source.sourceId
                                            .toString(),
                                    )
                                }

                                else ->
                                    throw IngestException(
                                        "Unsupported source type for streaming: ${source::class.simpleName}",
                                        isPermanent = true,
                                    )
                            }

                        val duration =
                            Duration.ofMillis(
                                System.currentTimeMillis() -
                                    attemptStartTime,
                            )
                        managedStreamingPolicy.streamingSuccessCallback(
                            source,
                            database,
                            table,
                            props,
                            ManagedStreamingRequestSuccessDetails(
                                duration,
                            ),
                        )

                        logger.info(
                            "Streaming ingestion succeeded for ${source::class.simpleName} on attempt $attempt. Duration: ${duration.toMillis()}ms",
                        )
                        response
                    } catch (e: Exception) {
                        logger.warn(
                            "Streaming ingestion attempt $attempt failed: ${e.message}",
                        )
                        throw e
                    }
                },
                onRetry = { attempt, ex, _ ->
                    logger.debug(
                        "Retrying streaming ingestion after attempt $attempt due to: ${ex.message}",
                    )
                    if (source is LocalSource) {
                        try {
                            source.reset()
                        } catch (e: Exception) {
                            logger.warn(
                                "Failed to reset source stream: ${e.message}",
                            )
                        }
                    }
                },
                shouldRetry = { attempt, ex, isPermanent ->
                    decideOnException(
                        source,
                        database,
                        table,
                        props,
                        startTime,
                        isPermanent,
                        ex,
                        attempt,
                    )
                },
                throwOnExhaustedRetries = false,
                tracer = { msg -> logger.debug(msg) },
            )

        if (result == null) {
            logger.info(
                "Streaming ingestion failed after retries, falling back to queued ingestion for ${source::class.simpleName}",
            )

            if (source is LocalSource) {
                try {
                    source.reset()
                } catch (e: Exception) {
                    logger.warn(
                        "Failed to reset source stream before queued ingestion: ${e.message}",
                    )
                }
            }
            return invokeQueuedIngestion(source, database, table, props)
        }

        return result
    }

    /** Decides whether to retry, throw, or break based on the exception */
    private fun decideOnException(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
        startTime: Long,
        isPermanent: Boolean,
        ex: Exception,
        attempt: UInt,
    ): Retry {
        val duration = Duration.ofMillis(System.currentTimeMillis() - startTime)

        // Handle transient errors
        if (!isPermanent) {
            reportTransientException(
                source,
                database,
                table,
                props,
                ex,
                duration,
            )
            return Retry(
                shouldRetry = true,
                interval = Duration.ZERO,
            )
        }

        // Handle permanent errors
        if (ex !is IngestException) {
            reportUnknownException(source, database, table, props, ex, duration)
            return Retry(
                shouldRetry = false,
                interval = Duration.ZERO,
            )
        }

        // Check if we should fallback to queued ingestion
        if (
            shouldFallbackToQueuedOnPermanentError(
                ex,
                source,
                database,
                table,
                props,
                duration,
            )
        ) {
            return Retry(
                shouldRetry = false,
                interval = Duration.ZERO,
            )
        }

        logger.error(
            "Permanent error occurred while trying streaming ingest, not switching to queued according to policy. Error: ${ex.message}",
        )
        return Retry(
            shouldRetry = false,
            interval = Duration.ZERO,
        )
    }

    /** Reports a transient exception to the policy */
    private fun reportTransientException(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
        ex: Exception,
        duration: Duration,
    ) {
        val errorCategory =
            if (
                ex is IngestException &&
                ex.failureCode ==
                HttpStatusCode.TooManyRequests.value
            ) {
                ManagedStreamingErrorCategory.THROTTLED
            } else {
                ManagedStreamingErrorCategory.OTHER_ERRORS
            }

        logger.warn(
            "Streaming ingestion transient error: ${ex.message}, category: $errorCategory",
        )

        managedStreamingPolicy.streamingErrorCallback(
            source,
            database,
            table,
            props,
            ManagedStreamingRequestFailureDetails(
                duration = duration,
                isPermanent = false,
                errorCategory = errorCategory,
                exception = ex,
            ),
        )
    }

    private fun reportUnknownException(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
        ex: Exception,
        duration: Duration,
    ) {
        logger.error(
            "Unexpected error occurred during streaming ingestion: ${ex.message}",
            ex,
        )

        managedStreamingPolicy.streamingErrorCallback(
            source,
            database,
            table,
            props,
            ManagedStreamingRequestFailureDetails(
                duration = duration,
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory.UNKNOWN_ERRORS,
                exception = ex,
            ),
        )
    }

    private fun shouldFallbackToQueuedOnPermanentError(
        ex: IngestException,
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
        duration: Duration,
    ): Boolean {
        val failureSubCode = ex.failureSubCode

        val errorCategory: ManagedStreamingErrorCategory
        val shouldFallback: Boolean

        when {
            // Streaming ingestion policy turned off
            failureSubCode?.contains(
                "StreamingIngestionPolicyNotEnabled",
                ignoreCase = true,
            ) == true ||
                failureSubCode?.contains(
                    "StreamingIngestionDisabledForCluster",
                    ignoreCase = true,
                ) == true -> {
                errorCategory =
                    ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF
                shouldFallback =
                    managedStreamingPolicy
                        .continueWhenStreamingIngestionUnavailable
                logger.info(
                    "Streaming ingestion is off, fallback to queued ingestion is " +
                        "${if (shouldFallback) "enabled" else "disabled"}. Error: ${ex.message}",
                )
            }

            // Table configuration prevents streaming
            failureSubCode?.contains(
                "UpdatePolicyIncompatible",
                ignoreCase = true,
            ) == true ||
                failureSubCode?.contains(
                    "QuerySchemaDoesNotMatchTableSchema",
                    ignoreCase = true,
                ) == true -> {
                errorCategory =
                    ManagedStreamingErrorCategory
                        .TABLE_CONFIGURATION_PREVENTS_STREAMING
                shouldFallback = true
                logger.info(
                    "Fallback to queued ingestion due to table configuration. Error: ${ex.message}",
                )
            }

            // Request properties prevent streaming (e.g., file too large)
            failureSubCode?.contains("FileTooLarge", ignoreCase = true) ==
                true ||
                failureSubCode?.contains(
                    "InputStreamTooLarge",
                    ignoreCase = true,
                ) == true ||
                ex.failureCode == 413 -> { // 413 Payload Too Large
                errorCategory =
                    ManagedStreamingErrorCategory
                        .REQUEST_PROPERTIES_PREVENT_STREAMING
                shouldFallback = true
                logger.info(
                    "Fallback to queued ingestion due to request properties. Error: ${ex.message}",
                )
            }

            else -> {
                errorCategory = ManagedStreamingErrorCategory.OTHER_ERRORS
                shouldFallback = false
                logger.info(
                    "Not falling back to queued ingestion for this exception: ${ex.message}",
                )
            }
        }

        managedStreamingPolicy.streamingErrorCallback(
            source,
            database,
            table,
            props,
            ManagedStreamingRequestFailureDetails(
                duration = duration,
                isPermanent = true,
                errorCategory = errorCategory,
                exception = ex,
            ),
        )

        return shouldFallback
    }

    private suspend fun invokeQueuedIngestion(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): IngestResponse {
        logger.info("Invoking queued ingestion for ${source::class.simpleName}")

        return queuedIngestionClient.submitQueuedIngestion(
            database = database,
            table = table,
            sources = listOf(source),
            format = props.format,
            ingestProperties = props,
        )
    }

    suspend fun getIngestionStatus(
        database: String,
        table: String,
        operationId: String,
        forceDetails: Boolean = false,
    ): StatusResponse {
        return queuedIngestionClient.getIngestionStatus(
            database,
            table,
            operationId,
            forceDetails,
        )
    }

    suspend fun pollUntilCompletion(
        database: String,
        table: String,
        operationId: String,
        pollingInterval: kotlin.time.Duration = 30.milliseconds,
        timeout: kotlin.time.Duration = 300.milliseconds,
    ): StatusResponse {
        return queuedIngestionClient.pollUntilCompletion(
            database = database,
            table = table,
            operationId = operationId,
            pollingInterval = pollingInterval,
            timeout = timeout,
        )
    }

    fun close() {
        // Clean up resources if needed
        logger.info("ManagedStreamingIngestClient closed")
    }
}

// Extension function to convert Kotlin Duration to Java Duration
private fun kotlin.time.Duration.toJavaDuration(): Duration {
    return Duration.ofMillis(this.inWholeMilliseconds)
}
