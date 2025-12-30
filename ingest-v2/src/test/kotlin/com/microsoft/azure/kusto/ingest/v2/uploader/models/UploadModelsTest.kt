// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.ingest.v2.uploader.models

import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Unit tests for upload models.
 */
class UploadModelsTest {

    // ==================== UploadErrorCode Tests ====================

    @Test
    fun `UploadErrorCode SOURCE_IS_NULL should have correct code and description`() {
        val errorCode = UploadErrorCode.SOURCE_IS_NULL
        assertEquals("UploadError_SourceIsNull", errorCode.code)
        assertEquals("Upload source is null", errorCode.description)
        assertEquals("UploadError_SourceIsNull", errorCode.toString())
    }

    @Test
    fun `UploadErrorCode SOURCE_NOT_FOUND should have correct code and description`() {
        val errorCode = UploadErrorCode.SOURCE_NOT_FOUND
        assertEquals("UploadError_SourceNotFound", errorCode.code)
        assertEquals("Upload source not found", errorCode.description)
    }

    @Test
    fun `UploadErrorCode SOURCE_NOT_READABLE should have correct code and description`() {
        val errorCode = UploadErrorCode.SOURCE_NOT_READABLE
        assertEquals("UploadError_SourceNotReadable", errorCode.code)
        assertEquals("Upload source is not readable", errorCode.description)
    }

    @Test
    fun `UploadErrorCode SOURCE_IS_EMPTY should have correct code and description`() {
        val errorCode = UploadErrorCode.SOURCE_IS_EMPTY
        assertEquals("UploadError_SourceIsEmpty", errorCode.code)
        assertEquals("Upload source is empty", errorCode.description)
    }

    @Test
    fun `UploadErrorCode SOURCE_SIZE_LIMIT_EXCEEDED should have correct code and description`() {
        val errorCode = UploadErrorCode.SOURCE_SIZE_LIMIT_EXCEEDED
        assertEquals("UploadError_SourceSizeLimitExceeded", errorCode.code)
        assertEquals("Upload source exceeds maximum allowed size", errorCode.description)
    }

    @Test
    fun `UploadErrorCode UPLOAD_FAILED should have correct code and description`() {
        val errorCode = UploadErrorCode.UPLOAD_FAILED
        assertEquals("UploadError_Failed", errorCode.code)
        assertEquals("Upload operation failed", errorCode.description)
    }

    @Test
    fun `UploadErrorCode NO_CONTAINERS_AVAILABLE should have correct code and description`() {
        val errorCode = UploadErrorCode.NO_CONTAINERS_AVAILABLE
        assertEquals("UploadError_NoContainersAvailable", errorCode.code)
        assertEquals("No upload containers available", errorCode.description)
    }

    @Test
    fun `UploadErrorCode CONTAINER_UNAVAILABLE should have correct code and description`() {
        val errorCode = UploadErrorCode.CONTAINER_UNAVAILABLE
        assertEquals("UploadError_ContainerUnavailable", errorCode.code)
        assertEquals("Upload container is unavailable", errorCode.description)
    }

    @Test
    fun `UploadErrorCode NETWORK_ERROR should have correct code and description`() {
        val errorCode = UploadErrorCode.NETWORK_ERROR
        assertEquals("UploadError_NetworkError", errorCode.code)
        assertEquals("Network error during upload", errorCode.description)
    }

    @Test
    fun `UploadErrorCode AUTHENTICATION_FAILED should have correct code and description`() {
        val errorCode = UploadErrorCode.AUTHENTICATION_FAILED
        assertEquals("UploadError_AuthenticationFailed", errorCode.code)
        assertEquals("Authentication failed for upload", errorCode.description)
    }

    @Test
    fun `UploadErrorCode UNKNOWN should have correct code and description`() {
        val errorCode = UploadErrorCode.UNKNOWN
        assertEquals("UploadError_Unknown", errorCode.code)
        assertEquals("Unknown upload error", errorCode.description)
    }

    @Test
    fun `UploadErrorCode values should return all error codes`() {
        val values = UploadErrorCode.values()
        assertEquals(11, values.size)
        assertTrue(values.contains(UploadErrorCode.SOURCE_IS_NULL))
        assertTrue(values.contains(UploadErrorCode.UNKNOWN))
    }

    @Test
    fun `UploadErrorCode valueOf should return correct enum`() {
        assertEquals(UploadErrorCode.SOURCE_IS_NULL, UploadErrorCode.valueOf("SOURCE_IS_NULL"))
        assertEquals(UploadErrorCode.UPLOAD_FAILED, UploadErrorCode.valueOf("UPLOAD_FAILED"))
    }

    // ==================== UploadResult.Success Tests ====================

    @Test
    fun `UploadResult Success should store all properties`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val success = UploadResult.Success(
            sourceName = "test-file.csv",
            startedAt = startTime,
            completedAt = endTime,
            blobUrl = "https://storage.blob.core.windows.net/container/blob",
            sizeBytes = 1024
        )

        assertEquals("test-file.csv", success.sourceName)
        assertEquals(startTime, success.startedAt)
        assertEquals(endTime, success.completedAt)
        assertEquals("https://storage.blob.core.windows.net/container/blob", success.blobUrl)
        assertEquals(1024, success.sizeBytes)
    }

    @Test
    fun `UploadResult Success should support data class copy`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val original = UploadResult.Success(
            sourceName = "original.csv",
            startedAt = startTime,
            completedAt = endTime,
            blobUrl = "https://original.blob",
            sizeBytes = 100
        )

        val copied = original.copy(sourceName = "copied.csv", sizeBytes = 200)

        assertEquals("copied.csv", copied.sourceName)
        assertEquals(200, copied.sizeBytes)
        assertEquals(original.blobUrl, copied.blobUrl)
    }

    @Test
    fun `UploadResult Success should support equals and hashCode`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val success1 = UploadResult.Success(
            sourceName = "file.csv",
            startedAt = startTime,
            completedAt = endTime,
            blobUrl = "https://blob",
            sizeBytes = 100
        )

        val success2 = UploadResult.Success(
            sourceName = "file.csv",
            startedAt = startTime,
            completedAt = endTime,
            blobUrl = "https://blob",
            sizeBytes = 100
        )

        assertEquals(success1, success2)
        assertEquals(success1.hashCode(), success2.hashCode())
    }

    // ==================== UploadResult.Failure Tests ====================

    @Test
    fun `UploadResult Failure should store all properties`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(5)
        val exception = RuntimeException("Test error")

        val failure = UploadResult.Failure(
            sourceName = "failed-file.csv",
            startedAt = startTime,
            completedAt = endTime,
            errorCode = UploadErrorCode.UPLOAD_FAILED,
            errorMessage = "Upload failed due to network error",
            exception = exception,
            isPermanent = true
        )

        assertEquals("failed-file.csv", failure.sourceName)
        assertEquals(startTime, failure.startedAt)
        assertEquals(endTime, failure.completedAt)
        assertEquals(UploadErrorCode.UPLOAD_FAILED, failure.errorCode)
        assertEquals("Upload failed due to network error", failure.errorMessage)
        assertEquals(exception, failure.exception)
        assertTrue(failure.isPermanent)
    }

    @Test
    fun `UploadResult Failure should have default isPermanent false`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(5)

        val failure = UploadResult.Failure(
            sourceName = "file.csv",
            startedAt = startTime,
            completedAt = endTime,
            errorCode = UploadErrorCode.NETWORK_ERROR,
            errorMessage = "Network timeout",
            exception = null
        )

        assertFalse(failure.isPermanent)
    }

    @Test
    fun `UploadResult Failure should allow null exception`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(5)

        val failure = UploadResult.Failure(
            sourceName = "file.csv",
            startedAt = startTime,
            completedAt = endTime,
            errorCode = UploadErrorCode.SOURCE_NOT_FOUND,
            errorMessage = "File not found",
            exception = null,
            isPermanent = true
        )

        assertNull(failure.exception)
    }

    @Test
    fun `UploadResult Failure should support data class copy`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(5)

        val original = UploadResult.Failure(
            sourceName = "original.csv",
            startedAt = startTime,
            completedAt = endTime,
            errorCode = UploadErrorCode.UPLOAD_FAILED,
            errorMessage = "Original error",
            exception = null,
            isPermanent = false
        )

        val copied = original.copy(errorMessage = "Copied error", isPermanent = true)

        assertEquals("Copied error", copied.errorMessage)
        assertTrue(copied.isPermanent)
        assertEquals(original.sourceName, copied.sourceName)
    }

    // ==================== UploadResults Tests ====================

    @Test
    fun `UploadResults should store successes and failures`() {
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

        assertEquals(2, results.successes.size)
        assertEquals(1, results.failures.size)
        assertEquals("file1.csv", results.successes[0].sourceName)
        assertEquals("file3.csv", results.failures[0].sourceName)
    }

    @Test
    fun `UploadResults should support empty lists`() {
        val results = UploadResults(emptyList(), emptyList())

        assertTrue(results.successes.isEmpty())
        assertTrue(results.failures.isEmpty())
    }

    @Test
    fun `UploadResults should support only successes`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val successes = listOf(
            UploadResult.Success("file.csv", startTime, endTime, "https://blob", 100)
        )

        val results = UploadResults(successes, emptyList())

        assertEquals(1, results.successes.size)
        assertTrue(results.failures.isEmpty())
    }

    @Test
    fun `UploadResults should support only failures`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val failures = listOf(
            UploadResult.Failure("file.csv", startTime, endTime, UploadErrorCode.UPLOAD_FAILED, "Error", null)
        )

        val results = UploadResults(emptyList(), failures)

        assertTrue(results.successes.isEmpty())
        assertEquals(1, results.failures.size)
    }

    @Test
    fun `UploadResults should support data class equality`() {
        val startTime = Instant.now()
        val endTime = startTime.plusSeconds(10)

        val successes = listOf(
            UploadResult.Success("file.csv", startTime, endTime, "https://blob", 100)
        )

        val results1 = UploadResults(successes, emptyList())
        val results2 = UploadResults(successes, emptyList())

        assertEquals(results1, results2)
        assertEquals(results1.hashCode(), results2.hashCode())
    }
}
