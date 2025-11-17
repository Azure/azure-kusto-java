// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2

import com.microsoft.azure.kusto.ingest.v2.common.CustomRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.common.SimpleRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.AbstractSourceInfo
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

/**
 * Error categories for managed streaming ingestion failures
 */
enum class ManagedStreamingErrorCategory {
    /**
     * Indicates that streaming cannot be performed due to the properties of the request itself
     * but would likely succeed if queued.
     * These errors are request-specific and do not imply anything about following requests.
     */
    REQUEST_PROPERTIES_PREVENT_STREAMING,

    /**
     * Indicates streaming cannot be performed due to a conflicting table configuration,
     * but may succeed if queued.
     * These errors are table-specific and following requests will behave similarly until
     * the conflict is resolved on the service side.
     */
    TABLE_CONFIGURATION_PREVENTS_STREAMING,

    /**
     * Indicates streaming cannot be performed due to some service configuration.
     * To resolve these errors, a service side change is required to use streaming.
     */
    STREAMING_INGESTION_OFF,

    /**
     * Indicates streaming ingestion endpoint is throttled and returns HTTP TooManyRequests error code (429)
     */
    THROTTLED,

    /**
     * Reported for all other types of streaming errors
     */
    OTHER_ERRORS,

    /**
     * Reported when an unexpected error type occurred
     */
    UNKNOWN_ERRORS,
}

/**
 * Details about a successful streaming ingestion request
 */
data class ManagedStreamingRequestSuccessDetails(
    val duration: Duration,
)

/**
 * Details about a failed streaming ingestion request
 */
data class ManagedStreamingRequestFailureDetails(
    val duration: Duration,
    val isPermanent: Boolean,
    val errorCategory: ManagedStreamingErrorCategory,
    val exception: Exception,
)

/**
 * A policy which controls the way the managed streaming ingest client behaves when there are errors.
 */
interface ManagedStreamingPolicy {
    /**
     * When streaming ingestion is disabled for the table, database or cluster, determine if the client will fallback to queued ingestion.
     * When set to false managed streaming client will fail ingestions for tables where streaming policy is not enabled.
     * Enabling this property means the client might use queued ingestion exclusively without the caller knowing.
     * Permanent errors in streaming ingestion that are not errors in queued ingestion, will fallback to queued ingestion regardless of this setting.
     */
    val continueWhenStreamingIngestionUnavailable: Boolean

    /**
     * The retry policy for transient failures before falling back to queued ingestion
     */
    val retryPolicy: IngestRetryPolicy

    /**
     * A size factor that enables tuning up and down the upper limit of data sent to streaming.
     * Default value is 1.0.
     */
    val dataSizeFactor: Double

    /**
     * Should this ingestion attempt skip streaming and go directly to queued ingestion
     *
     * @param source The ingestion source
     * @param database The target database name
     * @param table The target table name
     * @param props The ingestion properties
     * @return false if streaming should be attempted, true if streaming should be skipped
     */
    fun shouldDefaultToQueuedIngestion(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties?,
    ): Boolean

    /**
     * This callback will be called when a streaming error occurs
     *
     * @param source The ingestion source
     * @param database The target database name
     * @param table The target table name
     * @param props The ingestion properties
     * @param failureDetails Details about the failure
     */
    fun streamingErrorCallback(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties?,
        failureDetails: ManagedStreamingRequestFailureDetails,
    )

    /**
     * This callback will be called when streaming succeeds
     *
     * @param source The ingestion source
     * @param database The target database name
     * @param table The target table name
     * @param props The ingestion properties
     * @param successDetails Details about the success
     */
    fun streamingSuccessCallback(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties?,
        successDetails: ManagedStreamingRequestSuccessDetails,
    )
}

/**
 * This is the default policy used by the managed streaming ingestion client.
 * Whenever there is a permanent streaming error, it defaults to queued ingestion for a time period defined by timeUntilResumingStreamingIngest.
 */
class DefaultManagedStreamingPolicy(
    override val continueWhenStreamingIngestionUnavailable: Boolean = false,
    override val retryPolicy: IngestRetryPolicy = CustomRetryPolicy(
        arrayOf(
            Duration.ofSeconds(1).plusMillis(Random.nextLong(0, 1000)),
            Duration.ofSeconds(2).plusMillis(Random.nextLong(0, 1000)),
            Duration.ofSeconds(4).plusMillis(Random.nextLong(0, 1000)),
        ),
    ),
    override val dataSizeFactor: Double = 1.0,
    /**
     * When streaming is throttled, the client will fallback to queued ingestion.
     * This property controls how long the client will use queued ingestion in the case of streaming is throttled
     * before trying to resume streaming ingestion again.
     */
    val throttleBackoffPeriod: Duration = Duration.ofSeconds(10),
    /**
     * When streaming ingestion is unavailable, the client will fallback to queued ingestion.
     * This property controls how long the client will use queued ingestion before trying to resume streaming ingestion again.
     */
    val timeUntilResumingStreamingIngest: Duration = Duration.ofMinutes(15),
) : ManagedStreamingPolicy {

    private val defaultToQueuedUntilTimeByTable =
        ConcurrentHashMap<Pair<String, String>, Pair<Instant, ManagedStreamingErrorCategory>>()

    override fun shouldDefaultToQueuedIngestion(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties?,
    ): Boolean {
        val key = Pair(database, table)

        val useQueuedUntilTime = defaultToQueuedUntilTimeByTable[key]
        if (useQueuedUntilTime != null) {
            val (dateTime, errorCategory) = useQueuedUntilTime
            if (dateTime.isAfter(Instant.now())) {
                if (errorCategory == ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF &&
                    !continueWhenStreamingIngestionUnavailable
                ) {
                    return false
                }
                return true
            }
            defaultToQueuedUntilTimeByTable.remove(key)
        }

        return false
    }

    override fun streamingErrorCallback(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties?,
        failureDetails: ManagedStreamingRequestFailureDetails,
    ) {
        val key = Pair(database, table)
        when (failureDetails.errorCategory) {
            ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF,
            ManagedStreamingErrorCategory.TABLE_CONFIGURATION_PREVENTS_STREAMING,
            -> {
                defaultToQueuedUntilTimeByTable[key] =
                    Pair(
                        Instant.now().plus(timeUntilResumingStreamingIngest),
                        failureDetails.errorCategory,
                    )
            }

            ManagedStreamingErrorCategory.THROTTLED -> {
                defaultToQueuedUntilTimeByTable[key] =
                    Pair(
                        Instant.now().plus(throttleBackoffPeriod),
                        failureDetails.errorCategory,
                    )
            }

            else -> {
                // No action for other error categories
            }
        }
    }

    override fun streamingSuccessCallback(
        source: AbstractSourceInfo,
        database: String,
        table: String,
        props: IngestRequestProperties?,
        successDetails: ManagedStreamingRequestSuccessDetails,
    ) {
        // Default implementation does nothing
    }
}
