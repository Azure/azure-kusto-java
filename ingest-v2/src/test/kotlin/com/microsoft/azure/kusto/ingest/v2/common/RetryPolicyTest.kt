// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.common

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Unit tests for retry policy implementations. */
class RetryPolicyTest {

    @Test
    fun `NoRetryPolicy should never retry`() {
        val policy = NoRetryPolicy

        val retry1 = policy.moveNext(1u)
        assertFalse(retry1.shouldRetry)

        val retry2 = policy.moveNext(5u)
        assertFalse(retry2.shouldRetry)
    }

    @Test
    fun `SimpleRetryPolicy should retry with default intervals`() {
        val policy = SimpleRetryPolicy()

        val retry1 = policy.moveNext(1u)
        assertTrue(retry1.shouldRetry)
        assertEquals(Duration.ofSeconds(10), retry1.interval)

        val retry2 = policy.moveNext(2u)
        assertTrue(retry2.shouldRetry)
        assertEquals(Duration.ofSeconds(10), retry2.interval)
    }

    @Test
    fun `SimpleRetryPolicy should respect total retries`() {
        val policy = SimpleRetryPolicy(totalRetries = 3)

        assertTrue(policy.moveNext(1u).shouldRetry)
        assertTrue(policy.moveNext(2u).shouldRetry)
        assertTrue(policy.moveNext(3u).shouldRetry)
        assertFalse(policy.moveNext(4u).shouldRetry)
    }

    @Test
    fun `SimpleRetryPolicy should use custom interval duration`() {
        val customInterval = Duration.ofSeconds(5)
        val policy = SimpleRetryPolicy(intervalDuration = customInterval)

        val retry = policy.moveNext(1u)
        assertTrue(retry.shouldRetry)
        assertEquals(customInterval, retry.interval)
    }

    @Test
    fun `CustomRetryPolicy should use provided intervals`() {
        val intervals =
            arrayOf(
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5),
            )
        val policy = CustomRetryPolicy(intervals)

        assertEquals(Duration.ofSeconds(1), policy.moveNext(0u).interval)
        assertEquals(Duration.ofSeconds(2), policy.moveNext(1u).interval)
        assertEquals(Duration.ofSeconds(5), policy.moveNext(2u).interval)
        assertFalse(policy.moveNext(3u).shouldRetry)
    }

    @Test
    fun `CustomRetryPolicy with empty intervals should not retry`() {
        val policy = CustomRetryPolicy(arrayOf())

        assertFalse(policy.moveNext(0u).shouldRetry)
    }

    @Test
    fun `Retry data class should hold correct values`() {
        val retry1 = Retry(true, Duration.ofSeconds(5))
        assertTrue(retry1.shouldRetry)
        assertEquals(Duration.ofSeconds(5), retry1.interval)

        val retry2 = Retry(false, Duration.ZERO)
        assertFalse(retry2.shouldRetry)
        assertEquals(Duration.ZERO, retry2.interval)
    }
}
