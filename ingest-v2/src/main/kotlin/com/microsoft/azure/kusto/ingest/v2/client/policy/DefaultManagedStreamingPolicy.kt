// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client.policy

import com.microsoft.azure.kusto.ingest.v2.MANAGED_STREAMING_CONTINUE_WHEN_UNAVAILABLE_DEFAULT
import com.microsoft.azure.kusto.ingest.v2.MANAGED_STREAMING_DATA_SIZE_FACTOR_DEFAULT
import com.microsoft.azure.kusto.ingest.v2.MANAGED_STREAMING_RESUME_TIME_MINUTES
import com.microsoft.azure.kusto.ingest.v2.MANAGED_STREAMING_RETRY_DELAYS_SECONDS
import com.microsoft.azure.kusto.ingest.v2.MANAGED_STREAMING_RETRY_JITTER_MS
import com.microsoft.azure.kusto.ingest.v2.MANAGED_STREAMING_THROTTLE_BACKOFF_SECONDS
import com.microsoft.azure.kusto.ingest.v2.common.CustomRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

/**
 * This is the default policy used by the managed streaming ingestion client.
 * Whenever there is a permanent streaming error, it defaults to queued
 * ingestion for a time period defined by timeUntilResumingStreamingIngest.
 */
class DefaultManagedStreamingPolicy(
    override val continueWhenStreamingIngestionUnavailable: Boolean =
        MANAGED_STREAMING_CONTINUE_WHEN_UNAVAILABLE_DEFAULT,
    override val dataSizeFactor: Double =
        MANAGED_STREAMING_DATA_SIZE_FACTOR_DEFAULT,
    override val retryPolicy: IngestRetryPolicy =
        createDefaultRetryPolicy(),
    /**
     * When streaming is throttled, the client will fall back to queued
     * ingestion. This property controls how long the client will use queued
     * ingestion in the case of streaming is throttled before trying to
     * resume streaming ingestion again.
     */
    val throttleBackoffPeriod: Duration =
        Duration.ofSeconds(MANAGED_STREAMING_THROTTLE_BACKOFF_SECONDS),
    /**
     * When streaming ingestion is unavailable, the client will fall back to
     * queued ingestion. This property controls how long the client will use
     * queued ingestion before trying to resume streaming ingestion again.
     */
    val timeUntilResumingStreamingIngest: Duration =
        Duration.ofMinutes(MANAGED_STREAMING_RESUME_TIME_MINUTES),
) : ManagedStreamingPolicy {

    private val defaultToQueuedUntilTimeByTable =
        ConcurrentHashMap<String, ManagedStreamingErrorState>()

    /**
     * Determines whether to default to queued ingestion based on the current
     * error state for the specified table.
     */
    override fun shouldDefaultToQueuedIngestion(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): Boolean {
        val key = "$database-$table"

        val useQueuedUntilTime = defaultToQueuedUntilTimeByTable[key]
        if (useQueuedUntilTime != null) {
            val (dateTime, errorCategory) = useQueuedUntilTime
            if (dateTime.isAfter(Instant.now(Clock.systemUTC()))) {
                // If streaming is off, and we're not configured to continue, return false to fail
                return !(
                    errorCategory ==
                        ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF &&
                        !continueWhenStreamingIngestionUnavailable
                    )
            }
            // Time expired, remove the entry
            defaultToQueuedUntilTimeByTable.remove(key)
        }
        return false
    }

    override fun streamingErrorCallback(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        failureDetails: ManagedStreamingRequestFailureDetails,
    ) {
        val key = "$database-$table"

        when (failureDetails.errorCategory) {
            ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF,
            ManagedStreamingErrorCategory
                .TABLE_CONFIGURATION_PREVENTS_STREAMING,
            -> {
                defaultToQueuedUntilTimeByTable[key] =
                    ManagedStreamingErrorState(
                        Instant.now(Clock.systemUTC()) +
                            timeUntilResumingStreamingIngest,
                        failureDetails.errorCategory,
                    )
            }

            ManagedStreamingErrorCategory.THROTTLED -> {
                defaultToQueuedUntilTimeByTable[key] =
                    ManagedStreamingErrorState(
                        Instant.now(Clock.systemUTC()) +
                            throttleBackoffPeriod,
                        errorState = failureDetails.errorCategory,
                    )
            }
            else -> {
                // No action needed for other error categories
            }
        }
    }

    override fun streamingSuccessCallback(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        successDetails: ManagedStreamingRequestSuccessDetails,
    ) {
        // Default implementation does nothing
    }

    companion object {
        private val random = Random.Default

        /**
         * Creates a default retry policy with exponential backoff and jitter.
         * Uses delays from MANAGED_STREAMING_RETRY_DELAYS_SECONDS (1s, 2s, 4s)
         * plus random jitter (0-MANAGED_STREAMING_RETRY_JITTER_MS ms).
         */
        fun createDefaultRetryPolicy(): IngestRetryPolicy {
            // Create delays with jitter based on configured values
            val delays =
                MANAGED_STREAMING_RETRY_DELAYS_SECONDS.map { seconds ->
                    Duration.ofSeconds(seconds)
                        .plusMillis(
                            random.nextLong(
                                0,
                                MANAGED_STREAMING_RETRY_JITTER_MS,
                            ),
                        )
                }
                    .toTypedArray()
            return CustomRetryPolicy(delays)
        }

        /** Default instance with standard settings. */
        val DEFAULT = DefaultManagedStreamingPolicy()
    }
}
