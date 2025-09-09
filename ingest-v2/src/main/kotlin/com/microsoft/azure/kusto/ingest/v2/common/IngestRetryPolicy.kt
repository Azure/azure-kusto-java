/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2.common

import java.time.Duration

interface IngestRetryPolicy {
    /**
     * Determines whether the operation should be retried based on the
     * retryNumber. Returns a Pair<Boolean, Duration> indicating whether to
     * retry and the duration of the retry interval.
     */
    fun moveNext(retryNumber: UInt): Pair<Boolean, Duration>
}

object NoRetryPolicy : IngestRetryPolicy {
    override fun moveNext(retryNumber: UInt): Pair<Boolean, Duration> {
        return Pair(false, Duration.ZERO)
    }
}

class SimpleRetryPolicy(
    val intervalDuration: Duration = Duration.ofSeconds(10),
    val totalRetries: Int = 3,
) : IngestRetryPolicy {
    init {
        require(totalRetries > 0) { "totalRetries must be positive" }
    }

    override fun moveNext(retryNumber: UInt): Pair<Boolean, Duration> {
        require(retryNumber > 0u) { "retryNumber must be positive" }
        if (retryNumber >= totalRetries.toUInt()) {
            return Pair(false, Duration.ZERO)
        }
        return Pair(true, intervalDuration)
    }
}

class CustomRetryPolicy(intervalDurations: Array<Duration>? = null) :
    IngestRetryPolicy {
    private val intervalDurations: Array<Duration> =
        intervalDurations
            ?: arrayOf(
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                Duration.ofSeconds(7),
            )

    val intervals: List<Duration>
        get() = intervalDurations.toList()

    override fun moveNext(retryNumber: UInt): Pair<Boolean, Duration> {
        val idx = retryNumber.toInt()
        if (idx >= intervalDurations.size) {
            return Pair(false, Duration.ZERO)
        }
        return Pair(true, intervalDurations[idx])
    }
}
