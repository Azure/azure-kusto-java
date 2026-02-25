// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
package com.microsoft.azure.kusto.ingest.v2.client.policy

import com.microsoft.azure.kusto.ingest.v2.models.Format
import com.microsoft.azure.kusto.ingest.v2.models.IngestRequestProperties
import com.microsoft.azure.kusto.ingest.v2.source.CompressionType
import com.microsoft.azure.kusto.ingest.v2.source.StreamSource
import java.io.ByteArrayInputStream
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/** Unit tests for managed streaming policy classes. */
class ManagedStreamingPolicyTest {

    private fun createTestSource() =
        StreamSource(
            ByteArrayInputStream("test".toByteArray()),
            Format.csv,
            CompressionType.NONE,
        )

    private fun createTestProps() = IngestRequestProperties(format = Format.csv)

    // ==================== ManagedStreamingErrorCategory Tests ====================

    @Test
    fun `ManagedStreamingErrorCategory should have all expected values`() {
        val values = ManagedStreamingErrorCategory.values()
        assertEquals(6, values.size)
        assertTrue(
            values.contains(
                ManagedStreamingErrorCategory
                    .REQUEST_PROPERTIES_PREVENT_STREAMING,
            ),
        )
        assertTrue(
            values.contains(
                ManagedStreamingErrorCategory
                    .TABLE_CONFIGURATION_PREVENTS_STREAMING,
            ),
        )
        assertTrue(
            values.contains(
                ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF,
            ),
        )
        assertTrue(values.contains(ManagedStreamingErrorCategory.THROTTLED))
        assertTrue(values.contains(ManagedStreamingErrorCategory.OTHER_ERRORS))
        assertTrue(
            values.contains(ManagedStreamingErrorCategory.UNKNOWN_ERRORS),
        )
    }

    @Test
    fun `ManagedStreamingErrorCategory valueOf should return correct enum`() {
        assertEquals(
            ManagedStreamingErrorCategory.THROTTLED,
            ManagedStreamingErrorCategory.valueOf("THROTTLED"),
        )
        assertEquals(
            ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF,
            ManagedStreamingErrorCategory.valueOf("STREAMING_INGESTION_OFF"),
        )
    }

    // ==================== ManagedStreamingRequestSuccessDetails Tests ====================

    @Test
    fun `ManagedStreamingRequestSuccessDetails should store duration`() {
        val duration = Duration.ofSeconds(5)
        val details = ManagedStreamingRequestSuccessDetails(duration)

        assertEquals(duration, details.duration)
    }

    @Test
    fun `ManagedStreamingRequestSuccessDetails should support data class features`() {
        val details1 =
            ManagedStreamingRequestSuccessDetails(Duration.ofSeconds(5))
        val details2 =
            ManagedStreamingRequestSuccessDetails(Duration.ofSeconds(5))

        assertEquals(details1, details2)
        assertEquals(details1.hashCode(), details2.hashCode())
    }

    @Test
    fun `ManagedStreamingRequestSuccessDetails should support copy`() {
        val original =
            ManagedStreamingRequestSuccessDetails(Duration.ofSeconds(5))
        val copied = original.copy(duration = Duration.ofSeconds(10))

        assertEquals(Duration.ofSeconds(10), copied.duration)
    }

    // ==================== ManagedStreamingRequestFailureDetails Tests ====================

    @Test
    fun `ManagedStreamingRequestFailureDetails should store all properties`() {
        val duration = Duration.ofSeconds(3)
        val exception = RuntimeException("Test error")

        val details =
            ManagedStreamingRequestFailureDetails(
                duration = duration,
                isPermanent = true,
                errorCategory = ManagedStreamingErrorCategory.THROTTLED,
                exception = exception,
            )

        assertEquals(duration, details.duration)
        assertTrue(details.isPermanent)
        assertEquals(
            ManagedStreamingErrorCategory.THROTTLED,
            details.errorCategory,
        )
        assertEquals(exception, details.exception)
    }

    @Test
    fun `ManagedStreamingRequestFailureDetails should have default values`() {
        val exception = RuntimeException("Error")

        val details =
            ManagedStreamingRequestFailureDetails(
                isPermanent = false,
                exception = exception,
            )

        assertEquals(Duration.ZERO, details.duration)
        assertEquals(
            ManagedStreamingErrorCategory.OTHER_ERRORS,
            details.errorCategory,
        )
        assertFalse(details.isPermanent)
    }

    @Test
    fun `ManagedStreamingRequestFailureDetails errorCategory should be mutable`() {
        val exception = RuntimeException("Error")
        val details =
            ManagedStreamingRequestFailureDetails(
                isPermanent = false,
                exception = exception,
            )

        details.errorCategory =
            ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF

        assertEquals(
            ManagedStreamingErrorCategory.STREAMING_INGESTION_OFF,
            details.errorCategory,
        )
    }

    @Test
    fun `ManagedStreamingRequestFailureDetails should support data class features`() {
        val exception = RuntimeException("Error")
        val details1 =
            ManagedStreamingRequestFailureDetails(
                duration = Duration.ofSeconds(2),
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory.OTHER_ERRORS,
                exception = exception,
            )
        val details2 =
            ManagedStreamingRequestFailureDetails(
                duration = Duration.ofSeconds(2),
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory.OTHER_ERRORS,
                exception = exception,
            )

        assertEquals(details1, details2)
        assertEquals(details1.hashCode(), details2.hashCode())
    }

    // ==================== DefaultManagedStreamingPolicy Tests ====================

    @Test
    fun `DefaultManagedStreamingPolicy should have correct default values`() {
        val policy = DefaultManagedStreamingPolicy()

        // Default value from constants - continueWhenStreamingIngestionUnavailable defaults to
        // false
        assertFalse(policy.continueWhenStreamingIngestionUnavailable)
        assertEquals(1.0, policy.dataSizeFactor)
        assertNotNull(policy.retryPolicy)
    }

    @Test
    fun `DefaultManagedStreamingPolicy should accept custom values`() {
        val policy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable = false,
                dataSizeFactor = 0.5,
                throttleBackoffPeriod = Duration.ofMinutes(2),
                timeUntilResumingStreamingIngest =
                Duration.ofMinutes(30),
            )

        assertFalse(policy.continueWhenStreamingIngestionUnavailable)
        assertEquals(0.5, policy.dataSizeFactor)
        assertEquals(Duration.ofMinutes(2), policy.throttleBackoffPeriod)
        assertEquals(
            Duration.ofMinutes(30),
            policy.timeUntilResumingStreamingIngest,
        )
    }

    @Test
    fun `DefaultManagedStreamingPolicy shouldDefaultToQueuedIngestion returns false initially`() {
        val policy = DefaultManagedStreamingPolicy()

        val result =
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "testdb",
                "testtable",
                createTestProps(),
            )

        assertFalse(result)
    }

    @Test
    fun `DefaultManagedStreamingPolicy should queue after streaming ingestion off error`() {
        val policy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable = true,
                timeUntilResumingStreamingIngest = Duration.ofMinutes(1),
            )

        // Simulate streaming ingestion off error
        policy.streamingErrorCallback(
            createTestSource(),
            "testdb",
            "testtable",
            createTestProps(),
            ManagedStreamingRequestFailureDetails(
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory
                    .STREAMING_INGESTION_OFF,
                exception =
                RuntimeException("Streaming ingestion is off"),
            ),
        )

        // Should now default to queued
        val result =
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "testdb",
                "testtable",
                createTestProps(),
            )
        assertTrue(result)
    }

    @Test
    fun `DefaultManagedStreamingPolicy should queue after table configuration prevents streaming`() {
        val policy =
            DefaultManagedStreamingPolicy(
                timeUntilResumingStreamingIngest = Duration.ofMinutes(1),
            )

        policy.streamingErrorCallback(
            createTestSource(),
            "testdb",
            "testtable",
            createTestProps(),
            ManagedStreamingRequestFailureDetails(
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory
                    .TABLE_CONFIGURATION_PREVENTS_STREAMING,
                exception =
                RuntimeException(
                    "Table config prevents streaming",
                ),
            ),
        )

        val result =
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "testdb",
                "testtable",
                createTestProps(),
            )
        assertTrue(result)
    }

    @Test
    fun `DefaultManagedStreamingPolicy should queue after throttling`() {
        val policy =
            DefaultManagedStreamingPolicy(
                throttleBackoffPeriod = Duration.ofSeconds(30),
            )

        policy.streamingErrorCallback(
            createTestSource(),
            "testdb",
            "testtable",
            createTestProps(),
            ManagedStreamingRequestFailureDetails(
                isPermanent = false,
                errorCategory = ManagedStreamingErrorCategory.THROTTLED,
                exception = RuntimeException("Throttled"),
            ),
        )

        val result =
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "testdb",
                "testtable",
                createTestProps(),
            )
        assertTrue(result)
    }

    @Test
    fun `DefaultManagedStreamingPolicy should not queue for other errors`() {
        val policy = DefaultManagedStreamingPolicy()

        policy.streamingErrorCallback(
            createTestSource(),
            "testdb",
            "testtable",
            createTestProps(),
            ManagedStreamingRequestFailureDetails(
                isPermanent = false,
                errorCategory =
                ManagedStreamingErrorCategory.OTHER_ERRORS,
                exception = RuntimeException("Some other error"),
            ),
        )

        val result =
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "testdb",
                "testtable",
                createTestProps(),
            )
        assertFalse(result)
    }

    @Test
    fun `DefaultManagedStreamingPolicy should track errors by database and table`() {
        val policy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable = true,
                timeUntilResumingStreamingIngest = Duration.ofMinutes(1),
            )

        // Error on table1
        policy.streamingErrorCallback(
            createTestSource(),
            "db1",
            "table1",
            createTestProps(),
            ManagedStreamingRequestFailureDetails(
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory
                    .STREAMING_INGESTION_OFF,
                exception = RuntimeException("Off"),
            ),
        )

        // table1 should queue (continueWhenStreamingIngestionUnavailable = true)
        assertTrue(
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "db1",
                "table1",
                createTestProps(),
            ),
        )

        // table2 should not queue
        assertFalse(
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "db1",
                "table2",
                createTestProps(),
            ),
        )

        // Different database should not queue
        assertFalse(
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "db2",
                "table1",
                createTestProps(),
            ),
        )
    }

    @Test
    fun `DefaultManagedStreamingPolicy streamingSuccessCallback should not throw`() {
        val policy = DefaultManagedStreamingPolicy()

        // Should not throw
        policy.streamingSuccessCallback(
            createTestSource(),
            "testdb",
            "testtable",
            createTestProps(),
            ManagedStreamingRequestSuccessDetails(Duration.ofSeconds(1)),
        )
    }

    @Test
    fun `DefaultManagedStreamingPolicy should return false when streaming off and continueWhenUnavailable is false`() {
        val policy =
            DefaultManagedStreamingPolicy(
                continueWhenStreamingIngestionUnavailable = false,
                timeUntilResumingStreamingIngest = Duration.ofMinutes(1),
            )

        policy.streamingErrorCallback(
            createTestSource(),
            "testdb",
            "testtable",
            createTestProps(),
            ManagedStreamingRequestFailureDetails(
                isPermanent = true,
                errorCategory =
                ManagedStreamingErrorCategory
                    .STREAMING_INGESTION_OFF,
                exception =
                RuntimeException("Streaming ingestion is off"),
            ),
        )

        // Should return false (not queue, fail instead) when streaming is off and we don't continue
        val result =
            policy.shouldDefaultToQueuedIngestion(
                createTestSource(),
                "testdb",
                "testtable",
                createTestProps(),
            )
        assertFalse(result)
    }

    @Test
    fun `DefaultManagedStreamingPolicy createDefaultRetryPolicy should return valid policy`() {
        val retryPolicy =
            DefaultManagedStreamingPolicy.createDefaultRetryPolicy()

        assertNotNull(retryPolicy)
        // Should allow retries initially
        val retry = retryPolicy.moveNext(0u)
        assertTrue(retry.shouldRetry)
        assertTrue(retry.interval > Duration.ZERO)
    }

    @Test
    fun `DefaultManagedStreamingPolicy DEFAULT_MANAGED_STREAMING_POLICY should be accessible`() {
        val defaultPolicy =
            DefaultManagedStreamingPolicy.DEFAULT_MANAGED_STREAMING_POLICY

        assertNotNull(defaultPolicy)
        // Default value from constants is false
        assertFalse(defaultPolicy.continueWhenStreamingIngestionUnavailable)
    }
}
