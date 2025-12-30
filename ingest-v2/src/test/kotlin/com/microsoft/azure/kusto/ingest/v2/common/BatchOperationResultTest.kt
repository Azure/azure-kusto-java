// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.common

import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadErrorCode
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResult
import com.microsoft.azure.kusto.ingest.v2.uploader.models.UploadResults
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for BatchOperationResult interface.
 */
class BatchOperationResultTest {

    // Helper class implementing BatchOperationResult
    data class TestBatchResult<S, F>(
        override val successes: List<S>,
        override val failures: List<F>
    ) : BatchOperationResult<S, F>

    @Test
    fun `hasFailures should return true when there are failures`() {
        val result = TestBatchResult(
            successes = listOf("success1"),
            failures = listOf("failure1")
        )

        assertTrue(result.hasFailures)
    }

    @Test
    fun `hasFailures should return false when there are no failures`() {
        val result = TestBatchResult(
            successes = listOf("success1", "success2"),
            failures = emptyList<String>()
        )

        assertFalse(result.hasFailures)
    }

    @Test
    fun `allSucceeded should return true when there are no failures`() {
        val result = TestBatchResult(
            successes = listOf("success1", "success2"),
            failures = emptyList<String>()
        )

        assertTrue(result.allSucceeded)
    }

    @Test
    fun `allSucceeded should return false when there are failures`() {
        val result = TestBatchResult(
            successes = listOf("success1"),
            failures = listOf("failure1")
        )

        assertFalse(result.allSucceeded)
    }

    @Test
    fun `totalCount should return sum of successes and failures`() {
        val result = TestBatchResult(
            successes = listOf("s1", "s2", "s3"),
            failures = listOf("f1", "f2")
        )

        assertEquals(5, result.totalCount)
    }

    @Test
    fun `totalCount should return 0 for empty result`() {
        val result = TestBatchResult(
            successes = emptyList<String>(),
            failures = emptyList<String>()
        )

        assertEquals(0, result.totalCount)
    }

    @Test
    fun `totalCount should return only successes count when no failures`() {
        val result = TestBatchResult(
            successes = listOf("s1", "s2"),
            failures = emptyList<String>()
        )

        assertEquals(2, result.totalCount)
    }

    @Test
    fun `totalCount should return only failures count when no successes`() {
        val result = TestBatchResult(
            successes = emptyList<String>(),
            failures = listOf("f1", "f2", "f3")
        )

        assertEquals(3, result.totalCount)
    }

    // Test with UploadResults which implements BatchOperationResult
    @Test
    fun `UploadResults should implement BatchOperationResult correctly`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val successes = listOf(
            UploadResult.Success("file1.csv", startTime, endTime, "https://blob1", 100),
            UploadResult.Success("file2.csv", startTime, endTime, "https://blob2", 200)
        )

        val failures = listOf(
            UploadResult.Failure("file3.csv", startTime, endTime, UploadErrorCode.UPLOAD_FAILED, "Error", null)
        )

        val results = UploadResults(successes, failures)

        // Test BatchOperationResult interface methods
        assertEquals(2, results.successes.size)
        assertEquals(1, results.failures.size)
        assertTrue(results.hasFailures)
        assertFalse(results.allSucceeded)
        assertEquals(3, results.totalCount)
    }

    @Test
    fun `UploadResults with all successes should show allSucceeded true`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val successes = listOf(
            UploadResult.Success("file1.csv", startTime, endTime, "https://blob1", 100)
        )

        val results = UploadResults(successes, emptyList())

        assertFalse(results.hasFailures)
        assertTrue(results.allSucceeded)
        assertEquals(1, results.totalCount)
    }

    @Test
    fun `UploadResults with all failures should show hasFailures true`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val failures = listOf(
            UploadResult.Failure("file1.csv", startTime, endTime, UploadErrorCode.UPLOAD_FAILED, "Error", null)
        )

        val results = UploadResults(emptyList(), failures)

        assertTrue(results.hasFailures)
        assertFalse(results.allSucceeded)
        assertEquals(1, results.totalCount)
    }
}
