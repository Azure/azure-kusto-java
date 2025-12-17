// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client.policy

import com.microsoft.azure.kusto.ingest.v2.common.IngestRetryPolicy
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.IngestionSource
import java.time.Duration

/** Categories of errors that can occur during managed streaming ingestion. */
enum class ManagedStreamingErrorCategory {
    /**
     * Indicates that streaming cannot be performed due to the properties of the
     * request itself but would likely succeed if queued. These errors are
     * request specific and do not imply anything on following requests.
     */
    REQUEST_PROPERTIES_PREVENT_STREAMING,

    /**
     * Indicates streaming cannot be performed due to a conflicting table
     * configuration, but may succeed if queued. These errors are table specific
     * and following requests will behave similarly until the conflict is
     * resolved on the service side.
     */
    TABLE_CONFIGURATION_PREVENTS_STREAMING,

    /**
     * Indicates streaming cannot be performed due to some service
     * configuration. To resolve these errors, a service side change is required
     * to use streaming.
     */
    STREAMING_INGESTION_OFF,

    /**
     * Indicates streaming ingestion endpoint is throttled and returns HTTP
     * TooManyRequests error code (429).
     */
    THROTTLED,

    /** Reported for all other types of streaming errors. */
    OTHER_ERRORS,

    /** Reported when an unexpected error type occurred. */
    UNKNOWN_ERRORS,
}

/** Details about a successful streaming ingestion request. */
data class ManagedStreamingRequestSuccessDetails(val duration: Duration)

/** Details about a failed streaming ingestion request. */
data class ManagedStreamingRequestFailureDetails(
    val duration: Duration = Duration.ZERO,
    val isPermanent: Boolean,
    var errorCategory: ManagedStreamingErrorCategory =
        ManagedStreamingErrorCategory.OTHER_ERRORS,
    val exception: Exception,
)

/**
 * A policy which controls the way the managed streaming ingest client behaves
 * when there are errors.
 */
interface ManagedStreamingPolicy {
    /**
     * When streaming ingestion is disabled for the table, database or cluster,
     * determine if the client will fall back to queued ingestion. When set to
     * false managed streaming client will fail ingestions for tables where
     * streaming policy is not enabled. Enabling this property means the client
     * might use queued ingestion exclusively without the caller knowing.
     * Permanent errors in streaming ingestion that are not errors in queued
     * ingestion, will fall back to queued ingestion regardless of this setting.
     */
    val continueWhenStreamingIngestionUnavailable: Boolean

    /**
     * The number of times to attempt streaming data after transient failures,
     * before falling back to queued ingestion.
     */
    val retryPolicy: IngestRetryPolicy

    /**
     * A size factor that enables tuning up and down the upper limit of data
     * sent to streaming. Default value is 1.0.
     */
    val dataSizeFactor: Double

    /**
     * Should this ingestion attempt skip streaming and go directly to queued
     * ingestion.
     *
     * @return false if streaming should be attempted, true if streaming should
     *   be skipped
     */
    fun shouldDefaultToQueuedIngestion(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
    ): Boolean

    /** This callback will be called when a streaming error occurs. */
    fun streamingErrorCallback(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        failureDetails: ManagedStreamingRequestFailureDetails,
    )

    /** This callback will be called when streaming succeeds. */
    fun streamingSuccessCallback(
        source: IngestionSource,
        database: String,
        table: String,
        props: IngestRequestProperties,
        successDetails: ManagedStreamingRequestSuccessDetails,
    )
}
