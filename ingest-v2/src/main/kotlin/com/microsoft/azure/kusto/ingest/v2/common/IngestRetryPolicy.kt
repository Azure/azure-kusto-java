package com.microsoft.azure.kusto.ingest.v2.common

import java.time.Duration

/**
 * Represents a retry policy for ingesting data into Kusto.
 */
interface IngestRetryPolicy {
    /**
     * Determines whether the operation should be retried based on the retryNumber.
     * @param retryNumber The retry attempt number (zero-based).
     * @return Pair of (shouldRetry, retryInterval)
     */
    fun next(retryNumber: Int): Pair<Boolean, Duration>
}

/**
 * No retries will be attempted.
 */
object NoRetryPolicy : IngestRetryPolicy {
    override fun next(retryNumber: Int): Pair<Boolean, Duration> = Pair(false, Duration.ZERO)
}

/**
 * Simple retry policy with a constant duration between retry attempts.
 */
class SimpleRetryPolicy(
    val intervalDuration: Duration = Duration.ofSeconds(10),
    val totalRetries: Int = 3
) : IngestRetryPolicy {
    init {
        require(totalRetries > 0) { "totalRetries must be positive" }
    }
    override fun next(retryNumber: Int): Pair<Boolean, Duration> {
        require(retryNumber >= 0) { "retryNumber must be non-negative" }
        return if (retryNumber < totalRetries) Pair(true, intervalDuration)
        else Pair(false, Duration.ZERO)
    }
}

/**
 * Custom retry policy with a collection of interval durations between retry attempts.
 */
class CustomRetryPolicy(
    val intervalDurations: List<Duration> = listOf(
        Duration.ofSeconds(1),
        Duration.ofSeconds(3),
        Duration.ofSeconds(7)
    )
) : IngestRetryPolicy {
    override fun next(retryNumber: Int): Pair<Boolean, Duration> {
        return if (retryNumber < intervalDurations.size) {
            Pair(true, intervalDurations[retryNumber])
        } else {
            Pair(false, Duration.ZERO)
        }
    }
}
