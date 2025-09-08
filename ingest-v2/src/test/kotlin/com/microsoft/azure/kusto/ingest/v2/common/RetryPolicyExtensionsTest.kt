/* (C)2025 */
package com.microsoft.azure.kusto.ingest.v2.common

import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertEquals

class TestIngestRetryPolicy : IngestRetryPolicy {
    override fun moveNext(retryNumber: UInt): Pair<Boolean, java.time.Duration> {
        // Allow up to 3 attempts, 100ms delay
        return Pair(retryNumber < 3u, java.time.Duration.ofMillis(100))
    }
}

class RetryPolicyExtensionsTest {
    @Test
    fun testRunWithRetrySuccessAfterRetry() = runTest {
        val policy = TestIngestRetryPolicy()
        var callCount = 0
        val result =
            policy.runWithRetry(
                action = { attempt ->
                    callCount++
                    if (attempt < 2u) throw RuntimeException("Fail")
                    "Success"
                },
            )
        assertEquals(2, callCount)
        assertEquals("Success", result)
    }
}
