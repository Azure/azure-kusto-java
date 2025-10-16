// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay

enum class RetryDecision {
    Continue,
    ContinueWithoutDelay,
    Throw,
    Break,
}

suspend fun <T> IngestRetryPolicy.runWithRetry(
    action: suspend (UInt) -> T,
    // retry attempt number, exception, isPermanent
    onRetry: ((UInt, Exception, Boolean) -> Unit)? = null,
    // retry attempt number, exception, isPermanent
    onError: ((UInt, Exception, Boolean) -> Unit)? = null,
    shouldRetry: ((UInt, Exception, Boolean) -> Retry)? = null,
    throwOnExhaustedRetries: Boolean = true,
    tracer: ((String) -> Unit)? = null,
    cancellationChecker: (() -> Boolean)? = null,
): T? {
    var attempt: UInt = 1u
    while (true) {
        try {
            return action(attempt)
        } catch (ex: Exception) {
            val isPermanent = false // Stub: add logic if needed
            onError?.invoke(attempt, ex, isPermanent)
            val decision =
                shouldRetry?.invoke(attempt, ex, isPermanent)
                    ?: if (isPermanent) {
                        Retry.Throw
                    } else {
                        Retry.Continue
                    }

            when (decision) {
                Retry.Throw -> {
                    tracer?.invoke(
                        "Decision to throw on attempt $attempt. Is Permanent: $isPermanent. Exception: ${ex.message}",
                    )
                    throw ex
                }

                Retry.Break -> {
                    tracer?.invoke(
                        "Breaking out of retry loop early, on attempt $attempt. Exception: ${ex.message}",
                    )
                    return null
                }

                else -> {
                    val (shouldRetry, delayDuration) = this.moveNext(attempt)
                    if (!shouldRetry) {
                        tracer?.invoke(
                            "Retry policy exhausted on attempt $attempt. No more retries will be attempted. throwOnExhaustedRetries: $throwOnExhaustedRetries. Exception: ${ex.message}",
                        )
                        if (throwOnExhaustedRetries) throw ex
                        return null
                    }
                    tracer?.invoke(
                        "Transient error occurred: ${ex.message}. Retrying attempt $attempt.",
                    )
                    if (decision != Retry.ContinueWithoutDelay) {
                        if (delayDuration.toMillis() > 0) {
                            if (cancellationChecker?.invoke() == true) {
                                throw CancellationException(
                                    "Cancelled during retry delay",
                                )
                            }
                            delay(delayDuration.toMillis())
                        }
                    }
                    onRetry?.invoke(attempt, ex, isPermanent)
                }
            }
        }
        attempt++
    }
}
